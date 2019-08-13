package spark.sql

import org.apache.spark.sql.SparkSession

object HavingCount {

  val sparkSession: SparkSession = SparkSession.builder.master("local").getOrCreate()

  def main(args: Array[String]): Unit = {
    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0),
      ("o2", "u1", "2019-07-22", 120.0),
      ("o3", "u2", "2019-08-01", 100.0),
      ("o4", "u1", "2019-08-02", 220.0),


      ("o5", "u2", "2019-09-12", 220.0)

    )
    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
    orderDF.createOrReplaceTempView("orders")

    val orderDetails = Seq(
      ("o1", "ping_guo", "2019-07-01"),

      ("o2", "li", "2019-07-22"),
      ("o2", "banana", "2019-07-22"),
      ("o2", "ping_guo", "2019-07-22"),

      ("o3", "xi_gua", "2019-08-01"),

      ("o4", "banana", "2019-08-02"),

      ("o5", "banana", "2019-09-12")

    )
    val orderDetailDF = sparkSession.createDataFrame(orderDetails).toDF("order_id", "item", "item_time")
    orderDetailDF.createOrReplaceTempView("orderDetails")

    sparkSession.sql(
      """
        |
        |select *, row_number() over (partition by b.item order by b.order_time) rank
        |from
        |    (select * from  orders o join orderDetails d on o.order_id= d.order_id) b
        |
        |join
        |(
        | select member_id, item
        | from orders o join orderDetails d on o.order_id= d.order_id
        | group by member_id, item
        | having count(1) >1) t on t.member_id= b.member_id and t.item = b.item
        |
        |
      """.stripMargin)
      .show()
  }

}
