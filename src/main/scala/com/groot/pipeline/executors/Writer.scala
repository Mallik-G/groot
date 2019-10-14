package com.groot.pipeline.executors

import com.groot.pipeline.implementation.filesystem.WriteToFileSystem
import com.groot.yaml.classes.job_setup.JobSetup
import org.apache.spark.sql.DataFrame

object Writer {
  def execute(df: DataFrame, config: JobSetup): Unit = {
    if (config.output.target.toLowerCase == "file system") {
      WriteToFileSystem.run(df, config)
    }
  }
}
