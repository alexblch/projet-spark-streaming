package ESGI

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import java.nio.file.{Files, Paths}

object AppStreaming {
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

    val csvDirPath = "src/main/files/tmp/"

    // Lire les petits fichiers en streaming
    val fileData: DataFrame = spark.readStream
      .option("header", "true")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "true")
      .schema(schema)
      .csv(csvDirPath)

    println("Streaming data loaded")

    val query = fileData.writeStream
      .format("console")
      .outputMode("append")
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("10 seconds"))
      .start()

    query.awaitTermination()
  }
}
