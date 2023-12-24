package org.example

import org.apache.spark.sql.{Dataset, SparkSession}
import org.example.datasets.coursesList
import org.example.services.{SparkBusinessService, SparkDataBaseService}

object SparkScalaExampleDemo {
  println("inline 8 in object")

  def main(args: Array[String]): Unit = {
    println("inline 11 in object")


   // def fun (dbService: SparkDataBaseService, businessService: SparkBusinessService): Unit = {
//
//      import spark.implicits._
//      private val spark = getSpark(config.AppName)
      println("inline 18 in object")
      val df1 = businessService.coursesClass()
      val df2 = dbService.DBCources(df1)
      println("hey spark are your  working" + df2)


//      val spark = SparkSession.builder()
//      .appName("Hello")
//      .config("spark.master","local")
//     .enableHiveSupport()
//      .getOrCreate()
//
//    val sampleSeq = Seq((1,"SQL"),(2,"BIG DATA"),(3,"AWS"),(4,"SCALA"))
//    val df = spark.createDataFrame(sampleSeq).toDF("Course1","Course2")
//    println("hey spark are your  working")
//
//    df.show()
//    df.write.format("csv").save("file1")
//    spark.sql("CREATE TABLE IF NOT EXIST TABLE_CONTENTS" + "(key INT, value STRING) USING HIVE")

 // }

  }
}

