package org.rabbit.spark.functions

import org.rabbit.spark.sparkSession

object Mean{
  import sparkSession.implicits._
  def main(args: Array[String]): Unit = {
    val rddstr =sparkSession.sparkContext.parallelize(Seq(2,
      4,
      6))
    rddstr.toDF("id").createOrReplaceTempView("dates")


      sparkSession.sql(
      """
        |
        |select avg(id) , mean(id)
        |from dates
        |
        |
      """.stripMargin)
      .foreach(println(_))

  }

}
