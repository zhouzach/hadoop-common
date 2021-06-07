package org.rabbit.spark.monitor

import org.apache.spark.sql.SparkSession
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import scala.util.{Failure, Success, Try}

object MonitorStarter {

  protected val logger: Logger = LoggerFactory.getLogger(getClass)

  implicit val spark: SparkSession = SparkSession.builder
    .config("enableSendEmailOnTaskFail", "true")
    .config("org.rabbit.spark.extraListeners", "org.rabbit.spark.monitor.SparkJobListener")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    Try{
        val userData = Seq(("Leo", 16, 90), ("Marry", 21, 100), ("Jack", 14, 85), ("Tom", 16, 35), ("", 12, 62), (null, 22, 65))
        val dataFrame = spark.createDataFrame(userData).toDF("name", "age", "score")
        dataFrame.createOrReplaceTempView("users")

        spark.sql(
          """
            |
            |select di from users
            |""".stripMargin).show()
    } match {
      case Failure(ex) =>
        logger.error("task fail: ", ex)
        val end = DateTime.now().toString("yyyy-MM-dd HH:mm:ss.sss")
        MailHelper.send( new MailInfo (
          to = "769087026@qq.com":: Nil,
          subject = s"org.rabbit.spark job monitor on $end",
          message = ex.getLocalizedMessage
        ))

        println("task fail: " + s", on $end")
        println(s"error msg: ${ex.getMessage}")

      case Success(uint) =>
        val end = DateTime.now().toString("yyyy-MM-dd HH:mm:ss.sss")
        println(s"task finished: " + s", on $end")
    }
//    val rdd = org.rabbit.spark.sparkContext.parallelize(Seq(1, 2, 0, 3))
//    println(rdd.getNumPartitions)
//    rdd.foreach(d => 2 / d)
  }

}
