package spark.sql

import org.apache.spark.sql.functions.col
import spark.sparkSession

object GroupByCount {

  def main(args: Array[String]): Unit = {



    val orders = Seq(
      ("o1", "u1", "2019-07-11", 100.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)

    )
    sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
      .createOrReplaceTempView("orders")

    sparkSession.sql(
      """
        |
        |select member_id, 4 code, count(*)
        |from orders
        |group by member_id
        |
      """.stripMargin)
      .show()

    sparkSession.sql(
      """
        |
        |select member_id, 4 code, count(member_id)
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
