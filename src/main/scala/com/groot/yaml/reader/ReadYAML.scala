package com.groot.yaml.reader

import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor
import java.io.{FileInputStream, File}

import com.groot.yaml.classes.job_setup.JobSetup

object ReadYAML {
  def read(filepath: String): JobSetup = {
    val input = new FileInputStream(new File(filepath))
    val yaml = new Yaml(new Constructor(classOf[JobSetup]))
    yaml.load(input).asInstanceOf[JobSetup]
  }
}