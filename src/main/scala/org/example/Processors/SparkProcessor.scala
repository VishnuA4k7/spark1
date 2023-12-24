package org.example.Processors

import org.example.{ApplicationConfig, InitSpark}
import org.example.services.{SparkBusinessService, SparkDataBaseService}

class SparkProcessor (config: ApplicationConfig, dbService: SparkDataBaseService, businessService: SparkBusinessService) extends InitSpark{

  import spark.implicits._
  private val spark = getSpark(config.AppName)

  def process(): Unit = {

    val df1 = businessService.coursesClass()
    val df2 = dbService.DBCources(df1)
    println("hey spark are your  working" + df2)

  }
}
