package ESGI

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

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

    def replaceNullValues(df: DataFrame): DataFrame = {
      df.na.fill("N/A")
    }

    def addColumnPriceDouble(df: DataFrame): DataFrame = {
      df.withColumn("price_double", when(col("price").isNotNull && col("price") =!= "Price on request",
        regexp_replace(col("price"), "[$,]", "").cast("double"))
        .otherwise(0.0))
    }

    def calculateAveragePricesByBrand(data: DataFrame): DataFrame = {
      // Filter out rows where price_double is 0
      val watchDataWithDoublePrice = data.filter($"price_double" =!= 0)

      // Check if the dataset is empty
      if (watchDataWithDoublePrice.isEmpty) {
        println("No data to process for average prices.")
        return spark.emptyDataFrame
      }

      // Calculate the average price of all the dataset
      val averagePrice: Double = watchDataWithDoublePrice.select(avg($"price_double")).first().getDouble(0)
      // Calculate average prices by brand
      var averagePricesByBrand = watchDataWithDoublePrice.groupBy("brand").agg(avg($"price_double").alias("Prix moyen"))
      averagePricesByBrand = averagePricesByBrand.withColumn("Prix moyen", format_number($"Prix moyen", 2))

      println("Prix moyen de toutes les montres : " + averagePrice + " $")

      averagePricesByBrand
    }

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
          val averagePricesByBrand = calculateAveragePricesByBrand(transformedData)
          if (!averagePricesByBrand.isEmpty) {
            averagePricesByBrand.show()
          }

          Thread.sleep(5000) // Wait 5 seconds before selecting the next random batch
        }
      }
    }).start()

    query.awaitTermination()
  }
}
