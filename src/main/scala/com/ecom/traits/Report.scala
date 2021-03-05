package com.ecom.traits

import com.ecom.utils.{Constants, SparkUtils}
import org.apache.spark.sql.SparkSession

trait Report extends App {

  def run(spark: SparkSession, config: AppConfig, storage: Storage)

  def parseArgs(spark: SparkSession, args: Array[String]) = {
    new AppOptionParser().parse(args, AppConfig()) match {
      case Some(config) => run(spark, config, new S3Storage(spark))
      case None => throw new IllegalArgumentException("Invalid argument supplied")
    }
  }

  def appName: String

  override def main(args: Array[String]): Unit = {
    // Create spark session
    val spark = SparkUtils.getSparkSession(Constants.APPNAME)

    // parse arguments
    parseArgs(spark, args)

  }
}
