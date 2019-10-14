package com.groot.etl.transformations

import com.groot.conf.SessionSpark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

object RemoveDuplicates extends SessionSpark {
  def deduplicate(
                   df: DataFrame,
                   keyColumns: Array[String],
                   orderColumns: Array[String]
                 ): DataFrame = {

    val windowSpec = Window.partitionBy(keyColumns.map(col(_)): _*).orderBy(orderColumns.map(col(_)): _*)
    df
      .withColumn("internal_row_number", row_number().over(windowSpec))
      .filter("internal_row_number = 1")
      .drop("internal_row_number")
  }
}
