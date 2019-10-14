package com.groot.yaml.classes.job_setup

import scala.beans.BeanProperty

case class Output(){
  @BeanProperty var target = ""
  @BeanProperty var format = ""
  @BeanProperty var path = ""
  @BeanProperty var schema = ""
  @BeanProperty var table = ""
  @BeanProperty var partition_columns = Array("")
  @BeanProperty var unique_key = Array("")
  @BeanProperty var order_unique_key_by = Array("")
}
