package org.example

import org.apache.spark.sql.SparkSession

object SparkScalaExampleDemo {

  def main(args: Array[String]): Unit = {

   val spark = SparkSession.builder()
      .appName("Hello")
      .config("spark.master","local")
     .enableHiveSupport()
      .getOrCreate()

    val sampleSeq = Seq((1,"SQL"),(2,"BIG DATA"),(3,"AWS"),(4,"SCALA"))
    val df = spark.createDataFrame(sampleSeq).toDF("Course1","Course2")
    println("hey spark are your  working")

    df.show()
//    df.write.format("csv").save("file1")
//    spark.sql("CREATE TABLE IF NOT EXIST TABLE_CONTENTS" + "(key INT, value STRING) USING HIVE")

  }

  }
