package com.ecom.traits

import org.apache.spark.sql.{DataFrame, SparkSession}

trait Storage {
  def writeToStore(ds: DataFrame, path: String, saveMode: String, partitionMode: String)

  def readFromSource(path: String, config: AppConfig)
}

class S3Storage(spark:SparkSession) extends Storage{
  override def writeToStore(ds: DataFrame, path: String, saveMode: String, partitionMode: String): Unit = ???

  override def readFromSource(path: String, config: AppConfig): Unit = ???
}
