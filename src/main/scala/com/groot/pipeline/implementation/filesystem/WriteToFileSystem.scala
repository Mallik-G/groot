package com.groot.pipeline.implementation.filesystem

import com.groot.conf.SessionSpark
import com.groot.etl.writers.FsTable
import com.groot.pipeline.steps.Writer
import com.groot.yaml.classes.job_setup.JobSetup
import org.apache.spark.sql.DataFrame

object WriteToFileSystem extends Writer with SessionSpark {
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
    spark.sql(s"CREATE DATABASE IF NOT EXISTS ${dbName}")
  }

  def run(df: DataFrame, config: JobSetup): Unit = {
    createDatabase(config.output.schema)
    write(df, config)
  }
}