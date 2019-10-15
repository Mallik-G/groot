package com.groot.conf

import org.apache.spark.sql.SparkSession
import org.apache.spark.internal.Logging

trait SessionSpark extends Logging {
  log.info("Creating Spark Session")
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Groot")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.warehouse.dir", "\\meta_db")
    .enableHiveSupport()
    .getOrCreate()
}
