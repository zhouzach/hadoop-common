package spark.sql

import spark.sparkSession

object SumNull{

  def main(args: Array[String]): Unit = {



    val orders = Seq(
      ("o1", "u1", "m1","2019-07-11", null),
      ("o2", "u1", "m1","2019-07-02",null),
      ("o3", "u2", "m2","2019-06-01", null)

    )
    sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "m_code","order_time", "price")
      .createOrReplaceTempView("orders")


    sparkSession.sql(
      """
        |
        |select member_id, sum(price)
        |from orders
        |group by member_id
        |
      """.stripMargin)
      .show()

























//      .select("member_id","order_id")
//      .groupBy("member_id")
//      .count()
//      .filter(col("count").>(1))
//      .show()

  }

}
