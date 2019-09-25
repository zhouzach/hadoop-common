package spark.functions

import spark.sparkSession

object StdDate {
  import sparkSession.implicits._
  def main(args: Array[String]): Unit = {





    val rddstr =sparkSession.sparkContext.parallelize(Seq("2017-10-01 18:12:47","2017-10-02 18:12:47", "2017-10-03 18:12:47"))
    rddstr.toDF("cnt").createOrReplaceTempView("dates")


    sparkSession.sql(
      """
        |
        |select  stddev_pop(datediff(to_date('2017-10-03 18:12:47'), to_date(cnt)))
        |from dates
        |""".stripMargin)
        .show()




    val rdd =sparkSession.sparkContext.parallelize(Seq(0,1,2))

    import sparkSession.sqlContext.implicits._
    rdd.toDF("cnt").createOrReplaceTempView("nums")

    sparkSession.sql(
      """
        |
        |select
        |     stddev_pop(cnt) as stddev_pop_cnt
        |from nums
        |
        |
      """.stripMargin)
      .show()

  }

}
