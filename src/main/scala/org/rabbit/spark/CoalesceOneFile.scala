package org.rabbit.spark

import org.apache.spark.sql.SparkSession

object CoalesceOneFile {


  def mergeFiles(): Unit = {
    val filePattern = "/user/sqoop/part*"
    val outPut = ""

    val spark = SparkSession.builder.appName("Simple Application").getOrCreate()
//    val logData = org.rabbit.spark.read.textFile(logFile).cache()

    //    This will merge all part files into one and save it again into hdfs location
    spark.sparkContext.textFile(filePattern)
      .coalesce(1)
      .saveAsTextFile(outPut)


  }

}
