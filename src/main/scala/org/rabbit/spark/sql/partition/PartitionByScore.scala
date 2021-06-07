package org.rabbit.spark.sql.partition

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{DoubleType, IntegerType, StructField, StructType}
import org.rabbit.spark.sparkSession

object PartitionByScore{

  def main(args: Array[String]): Unit = {



    val someData = Seq(
      Row(1, 1.2),
      Row(2, 23.3),
      Row(3, 33.1),
      Row(4, 3.2),
      Row(5, 9.3),
      Row(6, 31.5),
      Row(7, 230.8),
      Row(8, 9.9),
      Row(9, 63.2),
      Row(10, 10.5)
    )
    val rdd = sparkSession.sparkContext.parallelize(someData)

    val someSchema = List(
      StructField("uid", IntegerType, true),
      StructField("score", DoubleType, true)
    )

    sparkSession.createDataFrame(
      rdd,
      StructType(someSchema)
    ).createOrReplaceTempView("scores")

    val cnt =1000
      sparkSession.sql(
      s"""
         |select *
         |from scores
         |order by score desc
         |
         |""".stripMargin).count()
    println(s"cnt: $cnt")

    sparkSession.sql(
      s"""
         |select *,
         |      case
         |      when num <= $cnt * 0.01 then 'A'
         |      when  num >= $cnt * 0.02 and num <= $cnt * 0.03 then 'B'
         |      when  num >= $cnt * 0.04 and num <= $cnt * 0.06 then 'C'
         |      when  num >= $cnt * 0.07 and num <= $cnt * 0.09 then 'D'
         |      when num > $cnt * 0.09 then 'E'
         |           else 'E' end as partition_num
         |from (select *, row_number() over ( order by score desc) as num from scores) t
         |
         |""".stripMargin)
      .show()

  }

}
