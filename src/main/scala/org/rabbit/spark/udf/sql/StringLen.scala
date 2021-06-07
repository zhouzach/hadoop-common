package org.rabbit.spark.udf.sql

import org.apache.spark.sql.SparkSession

object StringLen {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()


  def main(args: Array[String]): Unit = {

    sparkSession.udf.register("strLen", (str: String) => str.length())
    sparkSession.udf.register("isAdult", isAdult _)


    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    sparkSession.createDataFrame(userData).toDF("name", "age")
      .createOrReplaceTempView("user")

    sparkSession.sql("select name, strLen(name) as name_len, isAdult(age) as adult from user").show

  }

  def isAdult(age: Int) = {
    if (age < 18) {
      false
    } else {
      true
    }

  }

}
