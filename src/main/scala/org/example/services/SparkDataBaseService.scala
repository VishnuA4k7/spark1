package org.example.services

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset}
import org.example.datasets.coursesList
import org.example.{ApplicationConfig, InitSpark}

class SparkDataBaseService (config: ApplicationConfig) extends InitSpark{

  // test
  val spark = getSpark(config.AppName)
  import spark.implicits._

  def DBCources(totalList: Dataset[coursesList]): Unit = {
    println("inline 14 in db")
    // (s"""  SELECT * from ${totalList} where number in (2,4)""".stripMargin).as[coursesList]
    totalList.toDF().createOrReplaceTempView("temp_table1")
    val df = s"""  SELECT * FROM temp_table1 WHERE number in (2,4) """.stripMargin


  }

}

