package com.groot.etl.readers

import com.groot.conf.SessionSpark
import org.apache.spark.sql.DataFrame
import java.util

object FileReader extends SessionSpark {
  def read(
            fileFormat: String,
            schema: String,
            configuration: util.HashMap[String, String],
            filePath: String
          ): DataFrame = {
    spark
      .read
      .format(fileFormat)
      .schema(schema)
      .options(configuration)
      .load(filePath)
  }
}