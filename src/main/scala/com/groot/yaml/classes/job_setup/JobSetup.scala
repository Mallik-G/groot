package com.groot.yaml.classes.job_setup

import scala.beans.BeanProperty

case class JobSetup(){
  @BeanProperty var input = new Input()
  @BeanProperty var output = new Output()
}
