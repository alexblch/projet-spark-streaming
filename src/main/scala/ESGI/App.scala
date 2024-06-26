package ESGI

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import ESGI.Function.Utils._

object App {
  def main(args: Array[String]): Unit = {
    // Create a Spark session
    val spark = SparkSession.builder
      .appName("ESGIStreamingApp")
      .master("local[*]")
      .getOrCreate()
    println("Spark session started")

    import spark.implicits._
    // Reduce log level to avoid verbosity
    spark.sparkContext.setLogLevel("WARN")

    // Define the schema for the CSV file
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

    // Path to the directory containing the CSV files for streaming
    val csvDirPath = "src/main/files/"

    // Read the CSV data in streaming
    val watchData: DataFrame = spark.readStream
      .option("header", "true")
      .option("delimiter", ",")
      .option("quote", "\"")
      .option("escape", "\"")
      .option("multiLine", "true") // Handle newline characters within quoted values
      .schema(schema)
      .csv(csvDirPath)
    println("Streaming data loaded")

    // Define the batch size
    val batchSize = 10000

    // Define a streaming query to process data in batches of 10,000 rows every 5 seconds
    val query = watchData.writeStream
      .outputMode("append")
      .format("memory")
      .queryName("watches")
      .trigger(org.apache.spark.sql.streaming.Trigger.ProcessingTime("5 seconds"))
      .start()

    // Continuously process each micro-batch
    new Thread(new Runnable {
      override def run(): Unit = {
        while (true) {
          // Create a DataFrame from the temporary in-memory table
          val randomBatchData = spark.sql(s"SELECT * FROM watches ORDER BY RAND() LIMIT $batchSize")
          println(s"Number of rows: ${randomBatchData.count()}")

          // Add the price_double column and replace null values in the streaming DataFrame
          val transformedData = randomBatchData.transform(addColumnPriceDouble).transform(replaceNullValues)
          transformedData.show()

          println("Dataframe with average prices by brand:")
          val averagePricesByBrand = calculateAveragePricesByBrand(transformedData, spark)
          if (!averagePricesByBrand.isEmpty) {
            averagePricesByBrand.show()
          }

          val stddevByBrand = calculateStandardDeviationByBrand(transformedData)(spark)
          println("Dataframe with standard deviation by brand:")
          if (!stddevByBrand.isEmpty) {
            stddevByBrand.show()
          }

          val medianBrand = calculateMedianPricesByBrand(transformedData, spark)
          if(!medianBrand.isEmpty) {
            medianBrand.show()
          }

          val max_min = displayMaxMinPricesByBrand(transformedData, spark)
          if(!max_min.isEmpty) {
            max_min.show()
          }

          Thread.sleep(5000) // Wait 5 seconds before selecting the next random batch
        }
      }
    }).start()

    query.awaitTermination()
  }
}
