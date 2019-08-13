package spark.functions

import spark.sparkSession

object SortArray {
  def main(args: Array[String]): Unit = {
    val orders = Seq(
      ("o1", "u1", "2019-07-11", 100.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)

    )
    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
    orderDF.createOrReplaceTempView("orders")
    orderDF.orderBy("order_time","order_id")
        .show()

    sparkSession.sql(
      """
        |
        |select member_id, sort_array(collect_list(array(order_time,order_id)),true)
        |from orders
        |
        |group by member_id
        |
      """.stripMargin)
      .foreach(println(_))

  }

}
