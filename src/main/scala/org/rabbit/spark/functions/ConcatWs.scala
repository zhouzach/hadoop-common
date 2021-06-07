package org.rabbit.spark.functions

import org.apache.spark.sql.Row
import org.rabbit.spark.sparkSession

object ConcatWs {
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
        |select member_id, concat_ws(',',collect_list(order_id))
        |from orders
        |
        |group by member_id
        |
      """.stripMargin)
//      .foreach(println(_))

      .show()


    sparkSession.sql(
      """
        |
        |select member_id,
        |       collect_list(struct(order_id,price)) col1
        |from orders
        |
        |group by member_id
        |
      """.stripMargin)
      //      .foreach(println(_))
      .map{r =>
        val user = r.getAs[String]("member_id")
        val col1 = r.getAs[Seq[Row]]("col1").mkString("",",","")
        (user,col1)
      }.repartition(1)
      .write

      .csv("/Users/Zach/org.rabbit.hadoop-common/output/test2")

  }

}
