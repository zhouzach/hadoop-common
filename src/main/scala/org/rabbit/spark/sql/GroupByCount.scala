package org.rabbit.spark.sql

import org.apache.spark.sql.functions.col
import org.rabbit.spark.sparkSession

object GroupByCount {

  def main(args: Array[String]): Unit = {



    val orders = Seq(
      ("o1", "u1", "m1","2019-07-11", 100.0),
      ("o2", "u1", "m1","2019-07-02", 120.0),
      ("o3", "u2", "m2","2019-06-01", 100.0)

    )
    sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "m_code","order_time", "price")
      .createOrReplaceTempView("orders")

//    sparkSession.sql(
//      """
//        |
//        |select member_id, 4 code, count(*)
//        |from orders
//        |group by member_id
//        |
//      """.stripMargin)
//      .show()

    sparkSession.sql(
      """
        |
        |select member_id, 4 code, count(member_id) cnt, sum(case when price =120.0 then 1 else 0 end) /count(*)
        |from orders
        |group by member_id
        |
      """.stripMargin)
//      .show()


    val two = Seq(
      ("o1", "u1", "m1","2019-07-11", 100.0,3),
      ("o2", "u1", "m1","2019-07-02", 120.0,5),
      ("o3", "u2", "m2","2019-06-01", 100.0,7)
    )

    sparkSession.createDataFrame(two).toDF("order_id", "member_id", "m_code","order_time", "price","num")
      .createOrReplaceTempView("t_two")

    sparkSession.sql(
      """
        |
        |select sum(price) as prices, sum(num) as nums
        |from t_two
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
