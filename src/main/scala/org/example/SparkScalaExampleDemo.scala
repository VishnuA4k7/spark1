package org.example

import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.{Dataset, SparkSession}
import org.example.datasets.coursesList
import org.example.services.{SparkBusinessService, SparkDataBaseService}


object SparkScalaExampleDemo {

 def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("Hello").config("spark.master","local").enableHiveSupport()
          .getOrCreate()
   import spark.implicits._

    val database = "vishdb"
    val table = "dbo.Customer_Details_tbl"
    val user = "vishnu_a"                         // new login from SQL SERVER
    val password  = "Mi@2020ipl2"

    val jdbcDF = (spark.read.format("jdbc")
      .option("url",  "jdbc:sqlserver://VISHNU\\SQLEXPRESS:1433;databaseName=vishdb")
     .option("user", user)
      .option("password", password)
      .option("dbtable", table)
      .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .load())

    jdbcDF.show(truncate = false)

   val jdbcDF1 = (spark.read.format("jdbc")
     .option("url",  "jdbc:sqlserver://VISHNU\\SQLEXPRESS:1433;databaseName=vishdb")
     .option("user", user)
     .option("password", password)
     .option("dbtable", "[dbo].[Customer_Transactional_Details_tbl]")
     .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
     .load())

   jdbcDF1.show(truncate = false)


   val df2 = jdbcDF.join(jdbcDF1, jdbcDF("CustomerID") === jdbcDF1("CustomerID"), "inner")
   //val df3 = df2.select(expr("jdbcDF.CustomerID").as("ID"),"Customer_name","Product","Country","Description","jdbc.DateAdded")
   val df3= df2.show()




   // def fun (dbService: SparkDataBaseService, businessService: SparkBusinessService): Unit = {
//
//      import spark.implicits._
//     private val spark = getSpark(config.AppName)
   //   println("inline 18 in object")
     // val df1 = businessService.coursesClass()
   //   val df2 = dbService.DBCources(df1)
    //  println("hey spark are your  working" + df2)


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

