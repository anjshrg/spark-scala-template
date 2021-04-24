package com.ecom.schema


//UserID::MovieID::Rating::Timestamp
case class Rating(
                   userId: String,
                   movieId: String,
                   rating: Double,
                   timestamp: String
                 )

