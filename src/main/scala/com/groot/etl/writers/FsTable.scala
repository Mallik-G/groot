package com.groot.etl.writers

import com.groot.conf.SessionSpark
import org.apache.spark.sql.DataFrame
import io.delta.tables._
import org.apache.spark.sql.functions.col
import org.apache.spark.internal.Logging

object FsTable extends SessionSpark with Logging {
  def save(
            df: DataFrame,
            targetSchema: String,
            targetTable: String,
            targetTablePath: String,
            uniqueKey: Array[String],
            partitionColumns: Array[String]
          ): Unit = {

    log.info("Saving DataFrame as file system table")
    log.info(s"Target Schema=${targetSchema}")
    log.info(s"Target Table=${targetTable}")
    log.info(s"Table Path=${targetTablePath}")
    log.info(s"Table Unique Key=${uniqueKey}")
    log.info(s"Partition Columns=${partitionColumns}")

    val mergeCondition = createMergeCondition(uniqueKey)
    if (spark.catalog.tableExists(s"$targetSchema.$targetTable")) {
      log.info("Table already exists, merging data")
      DeltaTable.forPath(spark, targetTablePath)
        .as("old_data")
        .merge(
          df.as("new_data"),
          mergeCondition)
        .whenMatched
        .updateAll()
        .whenNotMatched
        .insertAll()
        .execute()
    } else {
      log.info("The table doesn't exist, creating a new one")
      df
        .repartition(partitionColumns.map(x => col(x)): _*)
        .write
        .partitionBy(partitionColumns: _*)
        .format("delta")
        .option("path", targetTablePath)
        .saveAsTable(s"$targetSchema.$targetTable")
    }
  }

  private def createMergeCondition(uniqueKey: Array[String]): String = {
    log.info("Creating merge condition")
    uniqueKey.map(key => s"old_data.$key = new_data.$key")
      .reduce((x, y) => s"$x AND $y")
  }

  private def analyzeTable(schemaName: String, tableName: String): Unit = {
    log.info("Analyzing table")
    spark.sql(s"ANALYZE TABLE $schemaName.$tableName COMPUTE STATISTICS")
  }
}