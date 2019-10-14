package com.groot.pipeline.implementation.filesystem

import com.groot.etl.readers.FileReader
import com.groot.pipeline.steps.Reader
import com.groot.yaml.classes.job_setup.JobSetup
import org.apache.spark.sql.DataFrame

object ReadFromFileSystem extends Reader {
  protected override def read(config: JobSetup): DataFrame = {
    FileReader.read(
      config.input.extension,
      config.input.schema,
      config.input.spark_conf,
      config.input.path
    )
  }

  def run(config: JobSetup): DataFrame = {
    dedupData(read(config), config)
  }
}
