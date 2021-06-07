package org.rabbit.spark.functions

import org.apache.spark.sql.Row
import org.rabbit.spark.sparkSession

object Cast{
  import sparkSession.sqlContext.implicits._
  def main(args: Array[String]): Unit = {
    val orders = Seq(
      ("o1", "u1", "2019-07-11", 100.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)

    )
    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
    orderDF.createOrReplaceTempView("orders")

    sparkSession.sql(
      """
        |
        |select price, cast(price as string) price_str
        |from orders
        |""".stripMargin)
      .show()







  }

}
