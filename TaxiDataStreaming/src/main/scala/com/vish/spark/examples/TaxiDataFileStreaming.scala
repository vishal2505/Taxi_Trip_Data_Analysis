package com.vish.spark.examples

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, ceil, concat, lit, window, desc}
import org.apache.spark.sql.types.{StructField, StringType, TimestampType, DoubleType, IntegerType, StructType}
import org.apache.spark.sql.streaming.Trigger

object TaxiDataFileStreaming extends Serializable {

  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  // Longitude and latitude from the upper left corner of the grid
  val init_long = -74.916578
  val init_lat = 41.47718278
  // Longitude and latitude from the lower right boundaries (+ 300)
  val limit_long = -73.120778
  val limit_lat = 40.12971598

  val inputSchema = StructType(List(
    StructField("medallion", StringType, true),
    StructField("hack_license", StringType, true),
    StructField("pickup_datetime", TimestampType, true),
    StructField("dropoff_datetime", TimestampType, true),
    StructField("trip_time_in_secs", IntegerType, true),
    StructField("trip_distance", DoubleType, true),
    StructField("pickup_longitude", DoubleType, true),
    StructField("pickup_latitude", DoubleType, true),
    StructField("dropoff_longitude", DoubleType, true),
    StructField("dropoff_latitude", DoubleType, true),
    StructField("payment_type", StringType, true),
    StructField("fare_amount", DoubleType, true),
    StructField("surcharge", DoubleType, true),
    StructField("mta_tax", DoubleType, true),
    StructField("tip_amount", DoubleType, true),
    StructField("tolls_amount", DoubleType, true),
    StructField("total_amount", DoubleType, true)))

  def pre_process_df(lines: DataFrame) =
  {
    //Filter empty rows
    var linesDF = lines.na.drop(how = "all")

    /* Filter Conditions
    1. pickup_longitude, pickup_latitude, dropoff_longitude, dropoff_latitude should be in the grid limit 300 x 300.
    2. trip time, trip distance and total amount are above 0.
     */
    linesDF = linesDF
      .filter(col("pickup_longitude") > init_long && col("pickup_longitude") < limit_long)
      .filter(col("pickup_latitude") < init_lat && col("pickup_latitude") > limit_lat)
      .filter(col("dropoff_longitude") > init_long && col("dropoff_longitude") < limit_long)
      .filter(col("dropoff_latitude") < init_lat && col("dropoff_latitude") > limit_lat)
      .filter(col("trip_time_in_secs") > 0)
      .filter(col("trip_distance") > 0.0)
      .filter(col("total_amount") > 0.0)

    //select only required columns
    linesDF = linesDF.selectExpr("dropoff_datetime", "dropoff_longitude", "dropoff_latitude", "pickup_longitude", "pickup_latitude")
    linesDF
  }

  def get_grid_df(preprocessedDF: DataFrame) = {

    //Longitude and latitude that correspond to a shift in 500 meters
    val long_shift = 0.005986
    val lat_shift = 0.004491556

    var gridDF = preprocessedDF
      .withColumn ("cell_pickup_longitude", ceil((col("pickup_longitude") - init_long) / long_shift) )
      .withColumn ("cell_pickup_latitude", - ceil ((col("pickup_latitude") - init_lat) / lat_shift) )
      .withColumn ("cell_dropoff_longitude", ceil ((col("dropoff_longitude") - init_long) / long_shift) )
      .withColumn ("cell_dropoff_latitude", - ceil ((col("dropoff_latitude") - init_lat) / lat_shift) )

    gridDF = gridDF
    .withColumn ("cell_pickup", concat(col("cell_pickup_longitude"), lit("-"), col("cell_pickup_latitude")))
    .withColumn ("cell_dropoff", concat(col("cell_dropoff_longitude"), lit("-"), col("cell_dropoff_latitude")))
    .drop ("cell_pickup_latitude", "cell_pickup_longitude", "cell_dropoff_latitude", "cell_dropoff_longitude")

    gridDF
  }


  // Gets the route from the concatenations of both cells

  def get_routes(lines: DataFrame) = {
    var routeDF = lines.withColumn("route", concat(col("cell_pickup"), lit("/"), col("cell_dropoff")))
    routeDF
  }

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[3]")
      .appName("Taxi Data File Streaming")
      .config("spark.sql.stopGracefullyOnShutdown", "true")
      .config("spark.sql.streaming.schemaInference", "true")
      .getOrCreate()

    val rawDF = spark.readStream
      .format("csv")
      .option("path", "input")
      .schema(inputSchema)
      .option("maxFilesPerTrigger", 1)
      .load()

    // Apply filters
    val preprocessedDF = pre_process_df(rawDF)

    // Get the cells from locations
    val gridDF = get_grid_df(preprocessedDF)

    // Join grids to form routes
    val routeDF = get_routes(gridDF)

    val most_frequent_routes = routeDF.withWatermark("dropoff_datetime", "30 minutes")
      .groupBy(window(col("dropoff_datetime"), "30 minutes"), col("route"))
      .count()
      .orderBy(desc("window"),desc("count"))
      .limit(10)

    logger.info("Most frequent routes")

    val query = most_frequent_routes.writeStream
      .format("console")
      .queryName("Taxi_Trip_Data_Streaming")
      .option("truncate",false)
      //.option("path", "output")
      .trigger(Trigger.ProcessingTime("30 seconds"))
      .outputMode("complete")
      .option("checkpointLocation", "chk-point-dir")
      .start()

      query.awaitTermination()

  }

}
