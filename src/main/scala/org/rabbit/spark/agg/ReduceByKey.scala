package org.rabbit.spark.agg

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Dataset, SparkSession}

/**
  * https://github.com/rohgar/scala-spark-4/wiki/Datasets
  *
  *
  * Grouped Operations on Datasets
  * Like on DataFrames, Datasets have special set of aggregation operations meant to be used after a call to groupByKey on a Dataset.
  *
  * calling groupByKey on a Dataset returns a KeyValueGroupedDataset
  * KeyValueGroupedDataset contains a no. of aggregation operations which return Datasets.
  * So, how to group and aggregate:
  *
  * call groupByKey on a Dataset and get back a KeyValueGroupedDataset
  * use an aggregation operation on KeyValueGroupedDataset and get back a Dataset.
  */
object ReduceByKey {

  val sparkSession: SparkSession = SparkSession.builder.master("local").getOrCreate()

  import sparkSession.sqlContext.implicits._

  def main(args: Array[String]): Unit = {


//    val orders = Seq(
//      ("o1", "u1", "2019-07-11", 100.0),
//      ("o2", "u1", "2019-07-02", 120.0),
//      ("o3", "u2", "2019-06-01", 100.0)
//
//    )
//    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
//    orderDF.createOrReplaceTempView("orders")
//
//    import sparkSession.implicits._
//    val kvDataset = orderDF.select("member_id", "order_id")
//      .map { r =>
//        (r.getAs[String]("member_id"), r.getAs[String]("order_id"))
//      }
//
//    reduceByKey4String(kvDataset)

    val userData = Seq((null,null))
    val df=sparkSession.createDataFrame(userData).toDF("name", "age")
        .map(r => (r.getString(0), r.getString(1)))
      reduceByKey4String(df)
  }


  def createFromRdd(rdd: RDD[String]) = {
    import sparkSession.sqlContext.implicits._

    rdd.toDS()
  }

  def reduceByKey(kvDataset: Dataset[(String, Double)]) = {
    import sparkSession.implicits._
    //    implicit val encoder = Encoders.kryo[String]
    //    implicit val e=Encoders.scalaDouble

    kvDataset.groupByKey(r => r._1) // KeyValueGroupedDataset[String, (String, Double)]
      //.mapGroups((k,pairs) =>())
      .mapValues(r => r._2) // KeyValueGroupedDataset[String, Double]
      .reduceGroups((acc, price) => acc + price) // Dataset[(String, Double)]
      .toDF("key", "agg")
      .show()
  }

  /**
    *
    *
    *
    *
    * @param kvDataset kvDataset can not be Seq((null,null)), values must not be null
    */
  def reduceByKey4String(kvDataset: Dataset[(String, String)]) = {
    import sparkSession.implicits._
    //    implicit val encoder = Encoders.kryo[String]
    //    implicit val e=Encoders.scalaDouble

    kvDataset
        .filter(null != _._2)
      .groupByKey(r => r._1) // KeyValueGroupedDataset[String, (String, String)]
      //.mapGroups((k,pairs) =>())
      .mapValues(r => r._2) // KeyValueGroupedDataset[String, String]
      .reduceGroups((acc, order) => acc + "|"+ order) // Dataset[(String, String)]
      .toDF("key", "agg")
      .show()
  }

}
