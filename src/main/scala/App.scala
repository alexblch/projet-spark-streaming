package ESGI

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths, StandardCopyOption}

object App {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("ESGIApp")
      .master("local[*]")
      .getOrCreate()

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

    val csvFilePath = "src/main/files/Watches.csv"
    val tmpDirPath = "src/main/files/tmp/"
    val streamingDirPath = "src/main/files/streaming/"

    // Nettoyer le répertoire de streaming
    Files.createDirectories(Paths.get(streamingDirPath)) // Crée le répertoire s'il n'existe pas
    Files.walk(Paths.get(streamingDirPath))
      .filter(Files.isRegularFile(_))
      .forEach(Files.deleteIfExists)
    println(s"Le répertoire $streamingDirPath a été nettoyé")

    // Lire le fichier CSV principal
    val data: DataFrame = spark.read
      .option("header", "true")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "true")
      .schema(schema)
      .csv(csvFilePath)

    println("CSV principal chargé")

    // Diviser le fichier en plusieurs petits fichiers de 5000 lignes aléatoires
    val numLinesPerFile = 5000
    val totalLines = data.count().toInt
    val numFiles = (totalLines.toDouble / numLinesPerFile).ceil.toInt

    for (i <- 0 until numFiles) {
      val randomLines = data.orderBy(rand()).limit(numLinesPerFile)
      val tempPath = s"$tmpDirPath/temp_$i"

      randomLines.write
        .option("header", "true")
        .mode("overwrite")
        .csv(tempPath)

      // Renommer le fichier créé pour correspondre à la structure souhaitée
      val tempFile = Files.list(Paths.get(tempPath)).filter(_.toString.endsWith(".csv")).findFirst().get()
      Files.move(tempFile, Paths.get(s"$tmpDirPath/part_$i.csv"), StandardCopyOption.REPLACE_EXISTING)
      Files.walk(Paths.get(tempPath)).sorted(java.util.Comparator.reverseOrder()).forEach(Files.deleteIfExists)
    }

    println(s"Les fichiers ont été divisés et écrits dans le répertoire tmp")
  }
}
