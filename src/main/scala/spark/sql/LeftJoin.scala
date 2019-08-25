package spark.sql

import org.apache.spark.sql.functions.{col, typedLit}
import spark.sparkSession

object LeftJoin {

  def main(args: Array[String]): Unit = {


    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)

    )
    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
      .createOrReplaceTempView("orders")

    val orderDetails = Seq(
      ("o1", "ping_guo"),
      ("o2", "li"),
      ("o2", "banana")
    )
    val orderDetailDF = sparkSession.createDataFrame(orderDetails).toDF("order_id", "item_name")
      .createOrReplaceTempView("details")

    sparkSession.sql(
      """
        |
        |select *
        |from orders d left join details t on  d.order_id=t.order_id
        |""".stripMargin)
      .show()

    sparkSession.sql(
      """
        |
        |select d.order_id, case when t.order_id is null then 0 else 1 end as label
        |from orders d left join details t on  d.order_id=t.order_id
        |""".stripMargin)
      .show()



  }

}
