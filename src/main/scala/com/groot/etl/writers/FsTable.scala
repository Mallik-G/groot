package com.groot.etl.writers

import com.groot.conf.SessionSpark
import org.apache.spark.sql.DataFrame
import io.delta.tables._
import org.apache.spark.sql.functions.col

object FsTable extends SessionSpark {
  def save(
            df: DataFrame,
            targetSchema: String,
            targetTable: String,
            targetTablePath: String,
            uniqueKey: Array[String],
            partitionColumns: Array[String]
          ): Unit = {

    val mergeCondition = createMergeCondition(uniqueKey)
    if (spark.catalog.tableExists(s"$targetSchema.$targetTable")) {
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
    uniqueKey.map(key => s"old_data.$key = new_data.$key")
      .reduce((x, y) => s"$x AND $y")
  }

  private def analyzeTable(schemaName: String, tableName: String): Unit = {
    spark.sql(s"ANALYZE TABLE $schemaName.$tableName COMPUTE STATISTICS")
  }
}