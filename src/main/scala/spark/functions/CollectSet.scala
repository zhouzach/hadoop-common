package spark.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{array, col, collect_set}

object CollectSet {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)

    )
    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
    //    orderDF.createOrReplaceTempView("orders")

    val orderDetails = Seq(
      ("o1", "ping_guo"),
      ("o2", "li"),
      ("o2", "banana"),
      ("o3", "xi_gua")
    )
    val orderDetailDF = sparkSession.createDataFrame(orderDetails).toDF("order_id", "item_id")
    //    orderDetailDF.createOrReplaceTempView("orderDetails")


    val joinDF = orderDF.alias("orders").join(orderDetailDF.alias("orderDetails"), orderDF("order_id") === orderDetailDF("order_id"))
//    joinDF.select("orders.order_id").show()
//    joinDF.select("orderDetails.order_id").show()
//    joinDF.show()
    /**
      * +--------+---------+----------+-----+--------+--------+
      * |order_id|member_id|order_time|price|order_id| item_id|
      * +--------+---------+----------+-----+--------+--------+
      * |      o1|       u1|2019-07-01|100.0|      o1|ping_guo|
      * |      o2|       u1|2019-07-02|120.0|      o2|  banana|
      * |      o2|       u1|2019-07-02|120.0|      o2|      li|
      * |      o3|       u2|2019-06-01|100.0|      o3|  xi_gua|
      * +--------+---------+----------+-----+--------+--------+
      */


    val groupSingleKeyDF = joinDF.groupBy("member_id").agg(collect_set(array(col("orders.order_id"),col("item_id"))).alias("item_list"))
    groupSingleKeyDF.show()
    groupSingleKeyDF.foreach(println(_))
  }

}
