package com.groot.pipeline.executors

import com.groot.pipeline.implementation.filesystem.ReadFromFileSystem
import com.groot.yaml.classes.job_setup.JobSetup
import org.apache.spark.sql.DataFrame

object Reader {
  def execute(config: JobSetup): DataFrame = {
    config.input.source.toLowerCase match {
      case "file system" => ReadFromFileSystem.run(config)
    }
  }
}
