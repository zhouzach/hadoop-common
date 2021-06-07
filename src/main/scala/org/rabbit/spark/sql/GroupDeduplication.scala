package org.rabbit.spark.sql

import org.rabbit.spark.sparkSession

object GroupDeduplication {


  def main(args: Array[String]): Unit = {



    val orders = Seq(
      ("o1", "u1", "m1","2019-07-11", 100.0),
      ("o2", "u1", "m2","2019-07-02", 120.0),
      ("o3", "u2", "m2","2019-06-01", 100.0)

    )
    sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "m_code","order_time", "price")
      .createOrReplaceTempView("orders")


    sparkSession.sql(
      """
        |
        |select member_id, m_code
        |from orders
        |group by member_id,m_code
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
