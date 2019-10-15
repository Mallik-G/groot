package com.groot.pipeline.executors

import com.groot.pipeline.implementation.filesystem.WriteToFileSystem
import com.groot.yaml.classes.job_setup.JobSetup
import org.apache.spark.sql.DataFrame
import org.apache.spark.internal.Logging

object Writer extends Logging {
  def execute(df: DataFrame, config: JobSetup): Unit = {
    log.info("Matching writer executor")
    if (config.output.target.toLowerCase == "file system") {
      WriteToFileSystem.run(df, config)
    }
  }
}
