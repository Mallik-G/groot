package com.groot.pipeline.executors

import com.groot.pipeline.implementation.filesystem.ReadFromFileSystem
import com.groot.yaml.classes.job_setup.JobSetup
import org.apache.spark.sql.DataFrame
import org.apache.spark.internal.Logging

object Reader extends Logging {
  def execute(config: JobSetup): DataFrame = {
    log.info("Matching source executor")
    config.input.source.toLowerCase match {
      case "file system" => ReadFromFileSystem.run(config)
    }
  }
}
