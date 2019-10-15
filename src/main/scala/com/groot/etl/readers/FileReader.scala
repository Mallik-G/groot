package com.groot.etl.readers

import com.groot.conf.SessionSpark
import org.apache.spark.sql.DataFrame
import java.util
import org.apache.spark.internal.Logging

object FileReader extends SessionSpark with Logging {
  def read(
            fileFormat: String,
            schema: String,
            configuration: util.HashMap[String, String],
            filePath: String
          ): DataFrame = {
    log.info("Reading file")
    log.info(s"File format=${fileFormat}")
    log.info(s"Schema=${schema}")
    log.info(s"Spark Conf=${configuration}")
    log.info(s"File Path=${filePath}")
    spark
      .read
      .format(fileFormat)
      .schema(schema)
      .options(configuration)
      .load(filePath)
  }
}