package org.rabbit.spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object DataFrameSql {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  val userData = Seq(("Leo", 16, 90), ("Marry", 21, 100), ("Jack", 14, 85), ("Tom", 16, 35), (null, 22, 65))
  val dataFrame = sparkSession.createDataFrame(userData).toDF("name", "age", "score")

  def main(args: Array[String]): Unit = {

    dataFrame
      .filter("name is not null")
      .orderBy("age", "score")
      .show()

    dataFrame
      .select("*")
      .where("name is not null")
      .orderBy(col("age"), col("score").desc)
      .show()
  }

}
