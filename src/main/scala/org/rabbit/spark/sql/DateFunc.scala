package org.rabbit.spark.sql

import org.apache.spark.sql.SparkSession

object DateFunc {

  val sparkSession: SparkSession = SparkSession.builder.master("local").getOrCreate()

  def main(args: Array[String]): Unit = {

    sparkSession.sql(
      """
        |
        | select year(current_timestamp()),
        | month(current_timestamp()),
        | day(current_timestamp()),
        | month(current_timestamp())
        |
        |
      """.stripMargin)
      .show()
  }

}
