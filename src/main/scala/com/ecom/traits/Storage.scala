package com.ecom.traits

import com.ecom.schema.Movie
import com.ecom.utils.{Constants, SparkUtils}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}

trait Storage {
  def writeToStore(ds: DataFrame, path: String, saveMode: String, partitionMode: String)

  def readFromSource(path: String, config: AppConfig)
}

class S3Storage(spark: SparkSession) extends Storage {
  override def writeToStore(ds: DataFrame, path: String, saveMode: String, partitionMode: String): Unit = ???

  override def readFromSource(path: String, config: AppConfig): Unit = ???
}

class LocalStorage(spark: SparkSession) extends Storage {
  def getMovieData(MOVIE_PATH: String) = {
    //val movieSchema = ScalaReflection.schemaFor[Movie].dataType.asInstanceOf[StructType]
    val encoderSchema = Encoders.product[Movie].schema
    SparkUtils.getSparkSession(Constants.APPNAME)
      .read.format("om.databricks.spark.csv")
      .schema(encoderSchema)
      .option("delimiter", "::")
      .csv(MOVIE_PATH)
  }

  override def writeToStore(ds: DataFrame, path: String, saveMode: String, partitionMode: String): Unit = {

  }

  override def readFromSource(path: String, config: AppConfig): Unit = {
    spark.read.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "::")
      .csv("/path/to/file")
  }
}
