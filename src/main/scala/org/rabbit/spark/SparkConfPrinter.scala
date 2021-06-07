package org.rabbit.spark

import org.apache.spark.sql.SparkSession

object SparkConfPrinter {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()


  def main(args: Array[String]): Unit = {

    get("org.rabbit.spark.executor.memoryOverhead")


  }


  def get(param: String)={
    println(s"$param: ${sparkSession.conf.get(param)}")
  }

  def getAll()={
    sparkSession.conf.getAll.foreach(println(_))
  }

  def showAll() = {
    sparkSession.sql("SET -v").show(numRows = 200, truncate = false)
  }
}
