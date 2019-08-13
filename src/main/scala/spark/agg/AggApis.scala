package spark.agg

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.apache.spark.sql.functions._

object AggApis {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  import sparkSession.sqlContext.implicits._

  def main(args: Array[String]): Unit = {

    val orders = Seq(
      ("o1", "u1", "i1", "2019-07-01", 100.0, 5, "hua wei"),
      ("o1", "u1", "i2", "2019-07-01", 100.0, 5, "ali"),
      ("o2", "u1", "i1", "2019-07-02", 120.0, 8, "bai du"),
      ("o3", "u2", "i3", "2019-06-01", 100.0, 10, "hua wei")
    )
    val orderDF = sparkSession.createDataFrame(orders)
      .toDF("order_id", "member_id", "item", "order_time", "price", "quantity", "brand")

    //    orderDF.groupBy("member_id")
    //      .agg(Map(
    //        "price" -> "max",
    //        "quantity" -> "avg"
    //      ))
    //      .show()
    //
    //    orderDF.groupBy("member_id") //RelationalGroupedDataset
    //      .agg(max("price"), sum("quantity"))
    //      .show()
    //
    //    val groupCollectList = orderDF.groupBy("member_id", "order_id")
    //      .agg(collect_list(col("item")).alias("items"), collect_list(col("brand")).alias("brands"))
    //      .show()
    //
    //    orderDF.groupByKey(r => r.getAs[String]("member_id")) //KeyValueGroupedDataset[String, Row]
    //      .agg(Aggregators.itemOfRowAgg.as[String])
    //      .show()


//    orderDF.groupByKey(r => (r.getAs[String]("member_id"), r.getAs[String]("order_id"))) //KeyValueGroupedDataset[String, Row]
//      .agg(Aggregators.multiFieldsOfRowAgg.as[(String, String)])
//      .toDF("user_order", "items_brands")
//      .map { r =>
//        val user_order = r.getAs[Row](0)
//        val user = user_order.getAs[String](0)
//        val order = user_order.getAs[String](1)
//
//        val items_brands = r.getAs[Row](1)
//        val items = items_brands.getAs[String](0)
//        val brands = items_brands.getAs[String](1)
//
//        (user, order, items, brands)
//      }
//      .collect()
//      .foreach(println(_))
//
//    orderDF.groupByKey(r => (r.getAs[String]("member_id"), r.getAs[String]("order_id"))) //KeyValueGroupedDataset[String, Row]
//      .agg(Aggregators.multiFieldsOfRowAgg.as[(String, String)])
//      .map { r =>
//        val user = r._1._1
//        val order = r._1._2
//        val items = r._2._1
//        val brands = r._2._2
//
//        (user, order, items, brands)
//      }
//      .collect()
//      .foreach(println(_))

    /**
      * +----------+--------------------+
      * |user_order|        items_brands|
      * +----------+--------------------+
      * |  [u1, o1]|[|i1|i2, |hua wei...|
      * |  [u1, o2]|      [|i1, |bai du]|
      * |  [u2, o3]|     [|i3, |hua wei]|
      * +----------+--------------------+
      */

    //    val reduceByKey = orderDF.groupByKey(r => (r.getAs[String]("member_id"), r.getAs[String]("order_id"))) //KeyValueGroupedDataset[(String, String), Row]
    //      .agg(Aggregators.itemOfRowAgg.as[String], Aggregators.brandOfRowAgg.as[String])
    //      .toDF("order_info", "items", "brands")
    //      .show()
    //
    //    val groupCount = orderDF.groupByKey(r => (r.getAs[String]("member_id"), r.getAs[String]("order_id"))) //KeyValueGroupedDataset[(String, String), Row]
    //      .count()
    //      .toDF("order_info", "count")
    //      .withColumn("addCol1", typedLit(1))
    //      .show()
    //
    //
    //    orderDF.groupByKey(r => (r.getAs[String]("member_id"), r.getAs[String]("order_id"))) //KeyValueGroupedDataset[(String, String), Row]
    //      .count()
    //      .toDF("order_info", "count")
    //      .show()

    orderDF.groupByKey(r => (r.getAs[String]("member_id"), r.getAs[String]("order_id"))) //KeyValueGroupedDataset[String, Row]
      .agg(Aggregators.collectListMultiAgg.as[(Seq[String], Seq[String])])
      .map { r =>
        val user = r._1._1
        val order = r._1._2
        val items = r._2._1
        val brands = r._2._2

        (user, order, items, brands)
      }
      .collect()
      .foreach(println(_))

  }


}


object Aggregators {
  val itemOfRowAgg = new Aggregator[Row, String, String] {

    import sparkSession.implicits._

    def zero: String = "" // Initial value - empty string

    def reduce(b: String, a: Row): String = {
      b + "|" + a.getAs[String]("item")
    } // Add an element to the running total

    def merge(b1: String, b2: String): String = {
      b1 + b2
    } // Merge Intermediate values

    def finish(r: String): String = r // final value

    override def bufferEncoder: Encoder[String] = newStringEncoder

    override def outputEncoder: Encoder[String] = newStringEncoder
  }.toColumn


  val brandOfRowAgg = new Aggregator[Row, String, String] {

    import sparkSession.implicits._

    def zero: String = "" // Initial value - empty string

    def reduce(b: String, a: Row): String = {
      b + "|" + a.getAs[String]("brand")
    } // Add an element to the running total

    def merge(b1: String, b2: String): String = {
      b1 + b2
    } // Merge Intermediate values

    def finish(r: String): String = r // final value

    override def bufferEncoder: Encoder[String] = newStringEncoder

    override def outputEncoder: Encoder[String] = newStringEncoder
  }.toColumn


  val multiFieldsOfRowAgg = new Aggregator[Row, (String, String), (String, String)] {

    import sparkSession.implicits._

    def zero: (String, String) = ("", "") // Initial value - empty string

    def reduce(b: (String, String), a: Row): (String, String) = {
      (b._1 + "|" + a.getAs[String]("item"),
        b._2 + "|" + a.getAs[String]("brand"))
    } // Add an element to the running total

    def merge(b1: (String, String), b2: (String, String)): (String, String) = {
      (b1._1 + b2._1, b1._2 + b2._2)
    } // Merge Intermediate values

    def finish(r: (String, String)): (String, String) = r // final value

    override def bufferEncoder: Encoder[(String, String)] = Encoders.product[(String, String)]

    override def outputEncoder: Encoder[(String, String)] = Encoders.product[(String, String)]
  }.toColumn


  val collectListStringAgg = new Aggregator[String, Seq[String], Seq[String]] {

    import sparkSession.implicits._

    def zero: Seq[String] = Seq.empty // Initial value - empty string
    def reduce(b: Seq[String], a: String): Seq[String] = {
      b :+ a
    } // Add an element to the running total

    def merge(b1: Seq[String], b2: Seq[String]): Seq[String] = {
      b1 ++ b2
    } // Merge Intermediate values
    def finish(r: Seq[String]): Seq[String] = r // final value
    override def bufferEncoder: Encoder[Seq[String]] = newSequenceEncoder

    override def outputEncoder: Encoder[Seq[String]] = newSequenceEncoder
  }.toColumn


  val collectListMultiAgg = new Aggregator[Row, (Seq[String], Seq[String]), (Seq[String], Seq[String])] {

    import sparkSession.implicits._

    def zero: (Seq[String], Seq[String]) = (Seq.empty, Seq.empty) // Initial value - empty string
    def reduce(b: (Seq[String], Seq[String]), a: Row): (Seq[String], Seq[String]) = {
      (b._1 :+ a.getAs[String]("item"),
        b._2 :+ a.getAs[String]("brand"))
    } // Add an element to the running total

    def merge(b1: (Seq[String], Seq[String]), b2: (Seq[String], Seq[String])): (Seq[String], Seq[String]) = {
      (b1._1 ++ b2._1, b1._2 ++ b2._2)
    } // Merge Intermediate values
    def finish(r: (Seq[String], Seq[String])): (Seq[String], Seq[String]) = r // final value
    override def bufferEncoder: Encoder[(Seq[String], Seq[String])] = Encoders.product[(Seq[String], Seq[String])]

    override def outputEncoder: Encoder[(Seq[String], Seq[String])] = Encoders.product[(Seq[String], Seq[String])]
  }.toColumn
}


case class MemberOrder(member: String, order: String)

