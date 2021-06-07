package org.rabbit.spark

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


package object agg {
  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()

  import sparkSession.sqlContext.implicits._



}
