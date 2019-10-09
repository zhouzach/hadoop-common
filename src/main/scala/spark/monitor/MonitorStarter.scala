package spark.monitor

import org.apache.spark.sql.SparkSession

object MonitorStarter {

  implicit val spark: SparkSession = SparkSession.builder
    .config("enableSendEmailOnTaskFail", "true")
    .config("spark.extraListeners", "spark.monitor.SparkJobListener")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    //    val userData = Seq(("Leo", 16, 90), ("Marry", 21, 100), ("Jack", 14, 85), ("Tom", 16, 35), ("", 12, 62), (null, 22, 65))
    //    val dataFrame = spark.createDataFrame(userData).toDF("name", "age", "score")
    //    dataFrame.createOrReplaceTempView("users")
    //
    //    spark.sql(
    //      """
    //        |
    //        |select di from users
    //        |""".stripMargin).show()

    val rdd = spark.sparkContext.parallelize(Seq(1, 2, 0, 3))
    println(rdd.getNumPartitions)
    rdd.foreach(d => 2 / d)
  }

}
