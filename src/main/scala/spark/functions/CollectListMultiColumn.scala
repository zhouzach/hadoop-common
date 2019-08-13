package spark.functions

import org.apache.spark.sql.functions.{array, col, collect_list}
import org.apache.spark.sql.{Encoders, SparkSession}

/**
  * https://www.mungingdata.com/apache-spark/arraytype-columns
  * https://docs.databricks.com/_static/notebooks/apache-spark-2.4-functions.html
  */
object CollectListMultiColumn {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()

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


    /**
      * https://stackoverflow.com/questions/42272532/spark-grouping-rows-in-array-by-key
      *
      * val assembler = new VectorAssembler()
      * .setInputCols(Array("key", "id", "val1", "val2", "val3","score"))
      * .setOutputCol("combined")
      *
      * val dfRes = assembler.transform(df).groupby("id").agg(collect_list($"combined"))
      */

    val df = orderDF.join(orderDetailDF, orderDF("order_id") === orderDetailDF("order_id"))
      .withColumn("combined", array("order_time", "price", "item_id"))
      .groupBy("member_id").agg(collect_list(col("combined")).alias("order_list"))
    //    df.withColumn("c", explode(col("combine"))).show()

    //    df.show()
    //    df.foreach(println(_))


//    val df2 = orderDF.join(orderDetailDF, orderDF("order_id") === orderDetailDF("order_id"))
//      .withColumn("combined", array("item_id"))
//      .groupBy("member_id")
//      .agg(flatten(collect_list(col("combined"))).alias("items"))
//
//    df2.foreach(println(_))


    val df3 = orderDF.join(orderDetailDF, orderDF("order_id") === orderDetailDF("order_id"))
      .withColumn("combined", array("item_id"))
      .groupBy("member_id")
      .agg(collect_list(col("combined")).alias("items"))

    implicit val pEncoder = Encoders.product[(String, Seq[String])]
    df3.map { r =>
      val user = r.getAs[String]("member_id")
      val items = r.getAs[Seq[Seq[String]]]("items")
      (user, items.flatten)
    }.foreach(println(_))

    //    val df3=df2.select("items")
    //      .collect()
    //      .flatMap(_.getAs[Seq[String]]("items"))
    //    df3.foreach(println(_))


    //    df.join(df2,"member_id").show()


  }

}
