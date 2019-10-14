package com.groot.pipeline.steps

import com.groot.yaml.classes.job_setup.JobSetup
import org.apache.spark.sql.DataFrame

trait Writer {
  protected def write(df: DataFrame,
            config: JobSetup
           ): Unit

  protected def createDatabase(dbName: String): Unit
}
