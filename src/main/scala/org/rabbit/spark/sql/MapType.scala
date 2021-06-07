package org.rabbit.spark.sql

import org.apache.spark.sql.SparkSession

object MapType {


  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()
  def main(args: Array[String]): Unit = {



    case class Test(name: String, m: Map[String, String])
    val map = Map("hello" -> "world", "hey" -> "there")
    val map2 = Map("hello" -> "people", "hey" -> "you")
    val rdd = sparkSession.sparkContext.parallelize(Seq(Test("first", map), Test("second", map2)))

    rdd.foreach(println(_))
//    import sparkSession.implicits._
//    val df = rdd.toDF
//    df.createOrReplaceTempView("mytable")
//    sparkSession.sql("desc mytable")
//    sparkSession.sql("select m.hello from mytable").show
  }

}
