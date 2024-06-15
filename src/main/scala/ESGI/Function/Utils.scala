package ESGI.Function

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Utils {

  def replaceNullValues(df: DataFrame): DataFrame = {
    df.na.fill("N/A")
  }

  def addColumnPriceDouble(df: DataFrame): DataFrame = {
    df.withColumn("price_double", when(col("price").isNotNull && col("price") =!= "Price on request",
      regexp_replace(col("price"), "[$,]", "").cast("double"))
      .otherwise(0.0))
  }

  def calculateAveragePricesByBrand(data: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

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

  def calculateMedianPricesByBrand(data: DataFrame, spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Filter out rows where price_double is 0
    val watchDataWithDoublePrice = data.filter($"price_double" =!= 0)

    // Check if the dataset is empty
    if (watchDataWithDoublePrice.isEmpty) {
      println("No data to process for median prices.")
      return spark.emptyDataFrame
    }
    val medianPrice: Double = watchDataWithDoublePrice.select(median($"price_double")).first().getDouble(0)
    println("Mediane de toutes les montres : " + medianPrice + " $")    // Calculate median prices by brand
    println("Dataframe mediane/marques :")
    val medianPricesByBrand = watchDataWithDoublePrice
      .groupBy("brand")
      .agg(expr("percentile_approx(price_double, 0.5)").alias("Prix médian"))
      .withColumn("Prix médian", format_number($"Prix médian", 2))

    medianPricesByBrand
  }

  def calculateStandardDeviationByBrand(data: DataFrame)(implicit spark: SparkSession): DataFrame = {
    import spark.implicits._

    // Calcul de l'écart-type des prix par marque
    var stddevByBrand = data.groupBy("brand")
      .agg(
        stddev($"price_double").alias("Ecart-type")
      )

    // Remplacer les valeurs null par 0
    stddevByBrand = stddevByBrand
      .withColumn("Ecart-type", coalesce($"Ecart-type", lit(0)))
      .withColumn("Ecart-type", format_number($"Ecart-type", 2))

    stddevByBrand
  }
}
