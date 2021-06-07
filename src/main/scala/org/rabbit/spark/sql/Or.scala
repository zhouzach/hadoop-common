package org.rabbit.spark.sql

import org.rabbit.spark.sparkSession

object Or{

  def main(args: Array[String]): Unit = {


    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)

    )
    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
      .createOrReplaceTempView("orders")

    sparkSession.sql(
      """
        |select * from orders
        |where order_time = '2019-07-02' or order_time = '2019-07-02'
        |""".stripMargin)
      .show()



  }

}
