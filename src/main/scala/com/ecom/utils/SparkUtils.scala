package com.ecom.utils

import org.apache.spark.sql.SparkSession

object SparkUtils {
  def getSparkSession(appName: String): SparkSession = {
    SparkSession.builder
      .appName(appName)
      //.config(//TODO: Pass config file)
      .enableHiveSupport
      .getOrCreate
  }

}
