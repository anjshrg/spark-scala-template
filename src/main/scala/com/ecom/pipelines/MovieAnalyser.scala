package com.ecom.pipelines

import com.ecom.schema.Movie
import com.ecom.traits.{AppConfig, LocalStorage, Report, Storage}
import com.ecom.utils.Constants
import org.apache.spark.sql.functions.{avg, col, explode, split}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.expressions.Window



//MovieID::Title::Genres


object MovieAnalyser extends Report {
  override def appName = "movie-etl-pipe"

  def joinWithMovieData(movieData: DataFrame):Dataset[Row] => Dataset[Row] = {
    df =>
      df.join(movieData,Seq("userId"),"left")
  }

  def sanitizeColumns:Dataset[Row] => Dataset[Row] = {
    movieDf =>
      // split array for genre
      movieDf.withColumn("genres",explode(split(col("genres"),"\\|")))
  }

  def computeTopMovies:Dataset[Row] => Dataset[Row] = {
    df =>
      val avgWindow = Window.partitionBy(col("movieId"),col("genres"))
      df
        .withColumn("average_rating",avg("ratings") over avgWindow)
  }

  def topRatedMoviesByGenre(movieData: DataFrame, ratingData: DataFrame) = {
    // Generate transformation chain for top rated movies by Genre
    ratingData.transform(joinWithMovieData(movieData))
      .transform(sanitizeColumns)
      .transform(computeTopMovies)
  }

  override def run(spark: SparkSession, config: AppConfig, storage: LocalStorage): Unit = {
    /*
case class Name(first:String,last:String,middle:String)
case class Employee(fullName:Name,age:Integer,gender:String)

import org.apache.spark.sql.catalyst.ScalaReflection
val scalaSchema = ScalaReflection.schemaFor[Employee].dataType.asInstanceOf[StructType]

val encoderSchema = Encoders.product[Employee].schema
encoderSchema.printTreeString()
 */
    // load data
    val movieData = storage.getMovieData(Constants.MOVIE_PATH)
    val userData = storage.getUserData(Constants.USER_PATH)
    val ratingData = storage.getRatingData(Constants.RATING_PATH)

    // transform data
    val result = topRatedMoviesByGenre(movieData,userData)

    // save data
    result.show(false)
  }
}