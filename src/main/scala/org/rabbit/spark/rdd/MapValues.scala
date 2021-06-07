package org.rabbit.spark.rdd

import org.apache.spark.sql.SparkSession

object MapValues {
  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val list = List("org.rabbit.hadoop", "org.rabbit.spark", "hive", "org.rabbit.spark")
    val rdd = sparkSession.sparkContext.parallelize(list)
    val pairRdd = rdd.map(x => (x, 1))
    pairRdd.mapValues(v => v + 1).collect.foreach(println) //对每个value进行+1
  }

}
