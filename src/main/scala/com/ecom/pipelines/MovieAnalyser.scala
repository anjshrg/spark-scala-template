package com.ecom.pipelines

import com.ecom.traits.{AppConfig, LocalStorage, Report, Storage}
import com.ecom.utils.Constants
import org.apache.spark.sql
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

//UserID::Gender::Age::Occupation::Zip-code
case class User(
                 userId: String,
                 gender: String,
                 age: Int,
                 occupation: String,
                 zipCode: String
               )

//UserID::MovieID::Rating::Timestamp
case class Rating(
                   userId: String,
                   movieId: String,
                   rating: Double,
                   timestamp: String
                 )

//MovieID::Title::Genres


object MovieAnalyser extends Report {
  override def appName = "movie-etl-pipe"

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

    // transform data
    // save data
  }

}