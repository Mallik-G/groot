package com.groot.yaml.classes.job_setup

import java.util

import scala.beans.BeanProperty

case class Input(){
  @BeanProperty var source = ""
  @BeanProperty var extension = ""
  @BeanProperty var compression = ""
  @BeanProperty var schema = ""
  @BeanProperty var spark_conf = new util.HashMap[String, String]()
  @BeanProperty var path = ""
}