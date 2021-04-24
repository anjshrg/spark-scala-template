package com.ecom.schema

//UserID::Gender::Age::Occupation::Zip-code
case class User(
                 userId: String,
                 gender: String,
                 age: Int,
                 occupation: String,
                 zipCode: String
               )