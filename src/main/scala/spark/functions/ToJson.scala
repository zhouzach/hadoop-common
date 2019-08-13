package spark.functions

import org.apache.spark.sql.functions.typedLit
import spark.sparkSession

object ToJson {

  def main(args: Array[String]): Unit = {

    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0),
      ("o2", "u1", "2019-07-02", 120.0),
      ("o3", "u2", "2019-06-01", 100.0)
    )

    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")

    orderDF.select("member_id")
      .withColumn("items1", typedLit(Seq(1, 2, 3, 4, 5)))
      .withColumn("items_double", typedLit(Seq(1.0, 2.1)))
//      .withColumn("to_json_array",to_json(struct(col("items_double"))))
//      .collect()
//      .foreach(println(_))
//      .show()

      .createOrReplaceTempView("orders")

    import sparkSession.sqlContext.implicits._
    sparkSession.sql(
      """
        |
        |select  items_double, to_json(struct(items_double)) as tojson from orders
      """.stripMargin)
//      .map { r =>
//        r.getAs[String]("tojson")
//      }
      .collect()
      .foreach(println(_))


  }

}
