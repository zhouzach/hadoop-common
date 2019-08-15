package spark.agg

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.sql.functions._

//case class MemberOrder(member: String, order: String)

case class RowInfo(member: String, order: String, prices: Seq[Double])

object GroupCollectList {

  def main(args: Array[String]): Unit = {

    import sparkSession.sqlContext.implicits._

    val orders = Seq(
      ("o1", "u1", "2019-07-11", 100.0),
      ("o1", "u1", "2019-07-11", 200.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)
    )
    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
    orderDF.createOrReplaceTempView("orders")


    val kvDataset = orderDF.select("member_id", "order_id", "price")
      .map { r =>
        (r.getAs[String]("member_id"), r.getAs[String]("order_id"), r.getAs[Double]("price"))
      }


    kvDataset.groupByKey(r => (r._1, r._2))
      .mapValues(v => v._3)
      .agg(collectListDoubleAgg.as[Seq[Double]])
    //      .show()


    orderDF
      //      .groupByKey(r => (r.getAs[String]("member_id"), r.getAs[String]("order_id")))
      .groupByKey(r => MemberOrder(r.getAs[String]("member_id"), r.getAs[String]("order_id")))
      .mapValues(r => r.getAs[Double]("price"))
      .agg(collectListDoubleAgg.as[Seq[Double]])
      .toDF("member_order_key", "prices")
      .map {
        r =>
          //          val row = r.getStruct(0)
          val row = r.getAs[Row](0)
          val user = row.getString(0)
          val order = row.getString(1)
          val ps = r.getAs[Seq[Double]](1)
          (user + "|" + order, ps)

      }
      .toDF("member_order", "prices")
      .withColumn("userArray",split(col("member_order"), "\\|"))
      .withColumn("user",split(col("member_order"), "\\|")(0))
      .withColumn("order",split(col("member_order"), "\\|")(1))
      .drop("member_order")
      //      .printSchema()
      .show()
    //      .foreach(println(_))


    /**
      * +--------------+---------+----+-----+
      * |        prices|userArray|user|order|
      * +--------------+---------+----+-----+
      * |[100.0, 200.0]| [u1, o1]|  u1|   o1|
      * |       [120.0]| [u1, o2]|  u1|   o2|
      * |       [100.0]| [u2, o3]|  u2|   o3|
      * +--------------+---------+----+-----+
      */


  }

  val collectListDoubleAgg = new Aggregator[Double, Seq[Double], Seq[Double]] {

    import sparkSession.implicits._

    def zero: Seq[Double] = Seq.empty // Initial value - empty string
    def reduce(b: Seq[Double], a: Double): Seq[Double] = {
      b :+ a
    } // Add an element to the running total

    def merge(b1: Seq[Double], b2: Seq[Double]): Seq[Double] = {
      b1 ++ b2
    } // Merge Intermediate values
    def finish(r: Seq[Double]): Seq[Double] = r // final value
    override def bufferEncoder: Encoder[Seq[Double]] = newSequenceEncoder

    override def outputEncoder: Encoder[Seq[Double]] = newSequenceEncoder
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


}
