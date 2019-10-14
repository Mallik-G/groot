package com.groot.pipeline.steps

import com.groot.etl.transformations.RemoveDuplicates
import com.groot.yaml.classes.job_setup.JobSetup
import org.apache.spark.sql.DataFrame

trait Reader {
  protected def read(config: JobSetup): DataFrame

  protected def dedupData(df: DataFrame,
                config: JobSetup
               ): DataFrame = {
    RemoveDuplicates.deduplicate(
      df,
      config.output.unique_key,
      config.output.order_unique_key_by)
  }
}