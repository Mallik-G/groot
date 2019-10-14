package com.groot.conf

import org.apache.spark.sql.SparkSession

trait SessionSpark {
  val spark = SparkSession
    .builder()
    .master("local")
    .appName("Groot")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .enableHiveSupport()
    .getOrCreate()
}
