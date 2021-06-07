package org.rabbit.spark.sql

import org.apache.spark.sql.functions.{col, typedLit}
import org.rabbit.spark.sparkSession

object JoinName {

  def main(args: Array[String]): Unit = {


    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)

    )
    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")

    val orderDetails = Seq(
      ("o1", "ping_guo"),
      ("o2", "li"),
      ("o2", "banana"),
      ("o3", "xi_gua")
    )
    val orderDetailDF = sparkSession.createDataFrame(orderDetails).toDF("order_id", "item_id")


    orderDF
//      .alias("orders")
      .join(orderDetailDF, orderDF("order_id") === orderDetailDF("order_id"))
      .select(orderDF("order_id"), col("member_id"))
      .withColumn("hello",typedLit(1))
      .show()
//    joinDF.select("orders.order_id").show()
  }

}
