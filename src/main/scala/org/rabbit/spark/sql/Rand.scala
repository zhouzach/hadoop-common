package org.rabbit.spark.sql

import org.apache.spark.sql.SparkSession

object Rand {

  val sparkSession: SparkSession = SparkSession.builder.master("local").getOrCreate()

  def main(args: Array[String]): Unit = {
    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0, 1),
      ("o2", "u1", "2019-07-22", 120.0, 2),
      (null, "u2", "2019-07-22", 120.0, 2),
      ("", "u2", "2019-07-22", 120.0, 2),
      ("o4", "u3", "2019-08-02", 220.0, -4)



    )
    val orderDF = sparkSession.createDataFrame(orders)
      .toDF("order_id", "member_id", "order_time", "price", "qty")
    orderDF.createOrReplaceTempView("orders")


    sparkSession.sql(
      """
        |
        | select rand() * 20
        |
        |
        |
        |
      """.stripMargin)
      .show()

    sparkSession.sql(
      """
        |
        | select
        | if(order_id is null or order_id = '', concat('nomid', cast(rand(1) as string)), order_id) as mid,member_id
        | from orders
        |
        |
      """.stripMargin)
      .show()


  }

}
