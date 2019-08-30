package spark.sql

import org.apache.spark.sql.SparkSession

object CaseWhen {

  val sparkSession: SparkSession = SparkSession.builder.master("local").getOrCreate()

  def main(args: Array[String]): Unit = {
    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0, 1),
      ("o2", "u1", "2019-07-22", 120.0, 2),
      ("o4", "u1", "2019-08-02", 220.0, -4)



    )
    val orderDF = sparkSession.createDataFrame(orders)
      .toDF("order_id", "member_id", "order_time", "price", "qty")
    orderDF.createOrReplaceTempView("orders")


    sparkSession.sql(
      """
        |
        | select member_id, case when sum(price) > 0 and sum(qty)>0  then 1 else 0 end as flag
        | from orders
        | group by member_id
        |
        |
      """.stripMargin)
      .show()

    sparkSession.sql(
      """
        |
        | select case when (price + qty) is not null  then price + qty else 0 end as flag
        | from orders
        |
        |
      """.stripMargin)
      .show()
  }

}
