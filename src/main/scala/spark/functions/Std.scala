package spark.functions

import spark.sparkSession

object Std {
  import sparkSession.implicits._
  def main(args: Array[String]): Unit = {

    val rdd =sparkSession.sparkContext.parallelize(Seq(0,1,2))

    import sparkSession.sqlContext.implicits._
    rdd.toDF("cnt").createOrReplaceTempView("nums")

    sparkSession.sql(
      """
        |
        |select
        |     stddev(cnt) as stddev_cnt,
        |     stddev_samp(cnt) as stddev_samp_cnt,
        |     stddev_pop(cnt) as stddev_pop_cnt,
        |     variance(cnt) as variance_cnt,
        |     var_samp(cnt) as var_samp_cnt,
        |     var_pop(cnt) as var_pop_cnt
        |
        |from nums
        |
        |
      """.stripMargin)
      .show()

    /**
     * 使用N所计算得到的方差及标准差只能用来表示该数据集本身(population)的离散程度；如果数据集是某个更大的研究对象的样本(sample)，
     * 那么在计算该研究对象的离散程度时，就需要对上述方差公式和标准差公式进行贝塞尔修正，将N替换为N-1。
     * 即是除以 N 还是 除以 N-1，则要看样本是否全：如果是抽样，则除以N-1，如果是全部，则除以N。
     * ————————————————
     * 原文链接：https://blog.csdn.net/u012328476/article/details/78436480
     *
     *  stddev, stddev_samp: n-1
     *  stddev_pop: n
     * +----------+---------------+-----------------+------------+------------+------------------+
     * |stddev_cnt|stddev_samp_cnt|   stddev_pop_cnt|variance_cnt|var_samp_cnt|       var_pop_cnt|
     * +----------+---------------+-----------------+------------+------------+------------------+
     * |       1.0|            1.0|0.816496580927726|         1.0|         1.0|0.6666666666666666|
     * +----------+---------------+-----------------+------------+------------+------------------+
     */

    val s = Seq(
      ("u1",0),
      ("u1",1),
      ("u1",2),
      ("u2",2),
      ("u2",3)
    )

    sparkSession.createDataFrame(s).toDF("user","cnt").createTempView("user_cnt")

    sparkSession.sql(
      """
        |
        |select
        |     stddev(cnt) as stddev_cnt,
        |     stddev_samp(cnt) as stddev_samp_cnt,
        |     stddev_pop(cnt) as stddev_pop_cnt,
        |     variance(cnt) as variance_cnt,
        |     var_samp(cnt) as var_samp_cnt,
        |     var_pop(cnt) as var_pop_cnt
        |
        |from user_cnt
        |group by user
        |
        |
      """.stripMargin)
      .show()

    /**
     *
     * +------------------+------------------+-----------------+------------+------------+------------------+
     * |        stddev_cnt|   stddev_samp_cnt|   stddev_pop_cnt|variance_cnt|var_samp_cnt|       var_pop_cnt|
     * +------------------+------------------+-----------------+------------+------------+------------------+
     * |               1.0|               1.0|0.816496580927726|         1.0|         1.0|0.6666666666666666|
     * |0.7071067811865476|0.7071067811865476|              0.5|         0.5|         0.5|              0.25|
     * +------------------+------------------+-----------------+------------+------------+------------------+
     */

  }

}
