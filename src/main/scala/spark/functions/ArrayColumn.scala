package spark.functions

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ArrayColumn {

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

    val itemDF = orderDF.select("member_id")
//      .withColumn("items1", typedLit(Seq(1, 2, 3, 4, 5)))
//      .withColumn("items2", typedLit(Seq(2, 3, 5)))
//      .withColumn("item1_size", size(col("items1")))
//      .withColumn("item1_head", col("items1").getItem(0))
//      .withColumn("contains", array_contains(col("items1"), 3))
      .withColumn("items3", struct("order_time", "price"))
      .withColumn("items3", array("order_time", "price"))
      .withColumn("items3", map(col("order_time"), col("price")))
      .withColumn("items3", typedLit(Seq("a",null)))
      .withColumn("contains", when(array_contains(col("items3"), "str"),true)
        .otherwise(false))

    itemDF.printSchema()
    itemDF.show()

//    implicit val encoder = Encoders.product[(String, Seq[(Int, Int)])]
//    val resutlDF = itemDF.map { r =>
//      val user = r.getAs[String]("member_id")
//      val items1 = r.getAs[Seq[Int]]("items1")
//      val items2 = r.getAs[Seq[Int]]("items2")
//      val map = items1.map { i =>
//        (i, if (items2.contains(i)) 1 else 0)
//      }
//      (user, map)
//    }
//    resutlDF.toDF("user", "map").foreach(println(_))
    //    resutlDF.write.save("file:////Users/Zach/hadoop-common/out")

  }

}
