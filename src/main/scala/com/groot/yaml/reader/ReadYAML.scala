package com.groot.yaml.reader

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import java.io.{FileInputStream, File}
import org.apache.spark.internal.Logging
import com.groot.yaml.classes.job_setup.JobSetup

object ReadYAML extends Logging {
  def read(filepath: String): JobSetup = {
    log.info("Reading YAML configuration file")
    val input = new FileInputStream(new File(filepath))
    val yaml = new Yaml(new Constructor(classOf[JobSetup]))
    yaml.load(input).asInstanceOf[JobSetup]
  }
}