package org.rabbit.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.rabbit.spark.sparkSession

object SumByMultiColumns{

  def main(args: Array[String]): Unit = {



    val someData = Seq(
      Row(8, 1.2),
      Row(2, 2.3),
      Row(27, 10.5)
    )
    val rdd = sparkSession.sparkContext.parallelize(someData)

    val someSchema = List(
      StructField("number", IntegerType, true),
      StructField("score", DoubleType, true)
    )

    sparkSession.createDataFrame(
      rdd,
      StructType(someSchema)
    ).createOrReplaceTempView("scores")

    sparkSession.sql(
      s"""
         |select sum(number) as cnt, sum(score) as scores
         |from scores
         |
         |""".stripMargin)
      .show()

  }

}
