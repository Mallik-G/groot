package com.groot.main

import com.groot.pipeline.executors.{Reader, Writer}
import com.groot.yaml.reader.ReadYAML

object Main {
  def main(args: Array[String]): Unit = {
    val config = ReadYAML.read(args(0))
    val dfSource = Reader.execute(config)
    Writer.execute(dfSource, config)
  }
}