package org.example

import org.apache.spark.sql.SparkSession
//
//trait InitSpark {
//  def spark(appName: String): SparkSession = SparkSession.builder()
//    .appName(appName)
//    .config("spark.sql.hive.convertMetastoreParquet","false")
//    .config("spark.sql.source.bucketing.enabled","true")
//    .config("spark.sql.crossJoin.enabled","true")
//    .config("spark.sql.parquet.writeLegacyFormat","true")
//    .enableHiveSupport()
//    .getOrCreate()
//
//}
