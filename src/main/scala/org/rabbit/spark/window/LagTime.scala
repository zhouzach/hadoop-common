package org.rabbit.spark.window

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object LagTime {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()


  def main(args: Array[String]): Unit = {


    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)

    )

    sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
      .createOrReplaceTempView("orders")

    sparkSession.sql(
      """
        |select  max(diff_time), min(diff_time)
        |from (
        |select order_time, lag_time, datediff(to_date(order_time), to_date(lag_time)) diff_time
        |from(
        | select order_time,
        |       lag(order_time, 1, 0) over (order by order_time)   lag_time
        | from orders)
        |) t
      """.stripMargin)
      .show()



  }

}
