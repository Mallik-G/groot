package com.groot.main

import com.groot.pipeline.executors.{Reader, Writer}
import com.groot.yaml.reader.ReadYAML
import org.apache.spark.internal.Logging

object Main extends Logging {
  def main(args: Array[String]): Unit = {
    log.info("Starting Application")
    val config = ReadYAML.read(args(0))
    val dfSource = Reader.execute(config)
    Writer.execute(dfSource, config)
    log.info("Application Finished")
  }
}