package com.groot.pipeline.implementation.filesystem

import com.groot.conf.SessionSpark
import com.groot.etl.writers.FsTable
import com.groot.pipeline.steps.Writer
import com.groot.yaml.classes.job_setup.JobSetup
import org.apache.spark.sql.DataFrame
import org.apache.spark.internal.Logging

object WriteToFileSystem extends Writer with SessionSpark with Logging {
  protected override def write(df: DataFrame, config: JobSetup): Unit = {
    FsTable.save(
      df,
      config.output.schema,
      config.output.table,
      config.output.path,
      config.output.unique_key,
      config.output.partition_columns
    )
  }

  protected override def createDatabase(dbName: String): Unit = {
    log.info("Creating hive database if it doesn't exist")
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${dbName}")
  }

  def run(df: DataFrame, config: JobSetup): Unit = {
    log.info("Executing file system writer")
    createDatabase(config.output.schema)
    write(df, config)
  }
}