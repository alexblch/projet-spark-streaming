package ESGI

import ESGI.Function.Utils._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, StandardCopyOption}
import scala.util.Random

object AppStreaming {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ESGIApp")
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._

    spark.sparkContext.setLogLevel("WARN")

    val schema = new StructType()
      .add("id", IntegerType)
      .add("name", StringType)
      .add("price", StringType)
      .add("brand", StringType)
      .add("model", StringType)
      .add("ref", StringType)
      .add("mvmt", StringType)
      .add("casem", StringType)
      .add("bracem", StringType)
      .add("yop", StringType)
      .add("cond", StringType)
      .add("sex", StringType)
      .add("size", StringType)
      .add("condition", StringType)

    val sourceDirPath = "src/main/files/tmp/"
    val streamingDirPath = "src/main/files/streaming/"

    // Nettoyer le répertoire de streaming
    Files.createDirectories(Paths.get(streamingDirPath)) // Crée le répertoire s'il n'existe pas
    Files.walk(Paths.get(streamingDirPath))
      .filter(Files.isRegularFile(_))
      .forEach(Files.deleteIfExists)
    println(s"Le répertoire $streamingDirPath a été nettoyé")

    // Lire les fichiers en streaming
    val fileData: DataFrame = spark.readStream
      .option("header", "true")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "true")
      .schema(schema)
      .csv(streamingDirPath)

    // Remplacer les valeurs NULL par N/A
    val nonNullData = replaceNullValues(fileData)

    // Ajouter la colonne price_double
    val dataWithPriceDouble = addColumnPriceDouble(nonNullData)

    // Afficher les DataFrames transformés
    val query = dataWithPriceDouble.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // Calculer et afficher les prix moyens par marque
    val avgPricesByBrand = dataWithPriceDouble
      .groupBy("brand")
      .agg(avg($"price_double").alias("Prix moyen"))
      .withColumn("Prix moyen", format_number($"Prix moyen", 2))

    avgPricesByBrand.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    // Calculer et afficher les prix médians par marque
    val medianPricesByBrand = dataWithPriceDouble
      .groupBy("brand")
      .agg(expr("percentile_approx(price_double, 0.5)").alias("Prix médian"))
      .withColumn("Prix médian", format_number($"Prix médian", 2))

    medianPricesByBrand.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    // Calculer et afficher l'écart-type des prix par marque
    val stddevByBrand = dataWithPriceDouble
      .groupBy("brand")
      .agg(stddev($"price_double").alias("Ecart-type"))
      .withColumn("Ecart-type", format_number($"Ecart-type", 2))

    stddevByBrand.writeStream
      .format("console")
      .outputMode("complete")
      .start()

    // Démarrer un thread pour copier les fichiers aléatoirement toutes les 15 secondes
    new Thread(new Runnable {
      def run(): Unit = {
        val files = new java.io.File(sourceDirPath).listFiles.filter(_.getName.endsWith(".csv")).map(_.getPath)
        while (true) {
          if (files.nonEmpty) {
            val randomFile = files(Random.nextInt(files.length))
            val sourcePath = Paths.get(randomFile)
            val destPath = Paths.get(streamingDirPath, sourcePath.getFileName.toString)
            Files.copy(sourcePath, destPath, StandardCopyOption.REPLACE_EXISTING)
            println(s"Copied file: $randomFile to $destPath")
          }
          Thread.sleep(15000) // Attendre 15 secondes
        }
      }
    }).start()

    query.awaitTermination()
  }
}
