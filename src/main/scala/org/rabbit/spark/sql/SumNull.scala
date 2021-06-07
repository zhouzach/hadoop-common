package org.rabbit.spark.sql

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.rabbit.spark.sparkSession

object SumNull{

  def main(args: Array[String]): Unit = {



//    val orders = Seq(
//      ("o1", "u1", "m1","2019-07-11", null),
//      ("o2", "u1", "m1","2019-07-02",null),
//      ("o3", "u2", "m2","2019-06-01", null)
//
//    )
//    sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "m_code","order_time", "price")
//      .createOrReplaceTempView("orders")
//
//
//    sparkSession.sql(
//      """
//        |
//        |select member_id, sum(nvl(price,0))
//        |from orders
//        |group by member_id
//        |
//      """.stripMargin)
//      .show()

//      .select("member_id","order_id")
//      .groupBy("member_id")
//      .count()
//      .filter(col("count").>(1))
//      .show()


    val someData = Seq(
      Row(8, "bat"),
      Row(null, "bat"),
      Row(null, null),
      Row(null, "a"),
      Row(null, "b"),
      Row(null, "b"),
      Row(27, "horse")
    )
    val rdd = sparkSession.sparkContext.parallelize(someData)

    val someSchema = List(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true)
    )

    sparkSession.createDataFrame(
      rdd,
      StructType(someSchema)
    ).createOrReplaceTempView("dict")

    sparkSession.sql(
      s"""
         |select word,sum(number) as cnt
         |from dict
         |group by word
         |""".stripMargin)
      .show()

  }

}
