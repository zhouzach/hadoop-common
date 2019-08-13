package spark.functions

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{struct, _}
import org.apache.spark.sql.types.StructType

object CollectList {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  import sparkSession.sqlContext.implicits._

  def main(args: Array[String]): Unit = {

    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)

    )
    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")

    val orderDetails = Seq(
      ("o1", "ping_guo"),
      ("o2", "li"),
      ("o2", "banana"),
      ("o3", "xi_gua")
    )
    val orderDetailDF = sparkSession.createDataFrame(orderDetails).toDF("order_id", "item_id")


    val joinDF = orderDF.join(orderDetailDF, "order_id")


    val groupSingleKeyDF = joinDF.groupBy("member_id").agg(collect_list(col("item_id")).alias("item_list"))
    //    groupSingleKeyDF.foreach(println(_))
    /**
      * [u1,WrappedArray(ping_guo, li, banana)]
      * [u2,WrappedArray(xi_gua)]
      */


    val groupMultiKeysDF1 = joinDF.groupBy("member_id").agg(collect_list(array("order_time", "price", "item_id")).alias("order_list"))
    //    groupMultiKeysDF1.printSchema()
    //    groupMultiKeysDF1.foreach(println(_))

    /**
      * root
      * |-- member_id: string (nullable = true)
      * |-- order_list: array (nullable = true)
      * |    |-- element: array (containsNull = true)
      * |    |    |-- element: string (containsNull = true)
      *
      * [u1,WrappedArray(WrappedArray(2019-07-01, 100.0, ping_guo), WrappedArray(2019-07-02, 120.0, banana), WrappedArray(2019-07-02, 120.0, li))]
      * [u2,WrappedArray(WrappedArray(2019-06-01, 100.0, xi_gua))]
      */


    val groupMultiKeysDF2 = joinDF.groupBy("member_id").agg(collect_list(struct("order_time", "price", "item_id")).alias("order_list"))
      .map { r =>
        val user = r.getAs[String](0)
        val seq = r.getAs[Seq[Row]](1)
          .map { rr =>
            (rr.getString(0), rr.getDouble(1), rr.getString(2))
          }
        (user, seq)
      }.collect()
      .foreach(println(_))

    //    groupMultiKeysDF2.printSchema()
    //    groupMultiKeysDF2.foreach(println(_))

    /**
      * root
      * |-- member_id: string (nullable = true)
      * |-- order_list: array (nullable = true)
      * |    |-- element: struct (containsNull = true)
      * |    |    |-- order_time: string (nullable = true)
      * |    |    |-- price: double (nullable = false)
      * |    |    |-- item_id: string (nullable = true)
      *
      * [u1,WrappedArray([2019-07-01,100.0,ping_guo], [2019-07-02,120.0,banana], [2019-07-02,120.0,li])]
      * [u2,WrappedArray([2019-06-01,100.0,xi_gua])]
      */

    //    groupMultiKeysDF.withColumn("c", explode(col("combine"))).show()


  }

}


object CollectList4sql {

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
    orderDF.createOrReplaceTempView("orders")

    val orderDetails = Seq(
      ("o1", "ping_guo"),
      ("o2", "li"),
      ("o2", "banana"),
      ("o3", "xi_gua")
    )
    val orderDetailDF = sparkSession.createDataFrame(orderDetails).toDF("order_id", "item_id")
    orderDetailDF.createOrReplaceTempView("orderDetails")

    sparkSession.sql(
      """
        |
        |select member_id,   array(o.order_id,item_id)
        |from orders o join orderDetails d on o.order_id = d.order_id
        |
        |
      """.stripMargin)
      .show()


    sparkSession.sql(
      """
        |
        |select member_id,   collect_list(array(o.order_id,item_id))
        |from orders o join orderDetails d on o.order_id = d.order_id
        |group by member_id
        |
      """.stripMargin)
      .foreach(println(_))


  }
}
