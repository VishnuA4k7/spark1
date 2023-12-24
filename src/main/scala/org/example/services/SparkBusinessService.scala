package org.example.services

import org.apache.spark.sql.Dataset
import org.example.datasets.coursesList
import org.example.{ApplicationConfig, InitSpark}

import scala.collection.mutable


class SparkBusinessService (config: ApplicationConfig) extends InitSpark {

  val spark = getSpark(config.AppName)
  import spark.implicits._

  def coursesClass():  Dataset[coursesList] = {

    println("Hey spark your in  business class")

   // val sampleSeq = Seq((1, "SQL"), (2, "BIG DATA"), (3, "AWS"), (4, "SCALA"))
    //val List = mutable.MutableList[coursesList]()
    println("inline 21 in business")

    val list1 = coursesList(1,"Spark")
    val list2 = coursesList(2,"SQL")
    val list3 = coursesList(3,"Scala")
    val list4 = coursesList(1,"AWS")

   // val totalList = mutable.MutableList[coursesList]()
   val  totalList = List (list1,list2,list3,list4)
    totalList.toDS()



  }
}
