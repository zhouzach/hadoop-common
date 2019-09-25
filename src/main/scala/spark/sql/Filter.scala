package spark.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Filter {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  val userData = Seq(("Leo", 16, 90), ("Marry", 21, 100), ("Jack", 14, 85), ("Tom", 16, 35), (null, 22, 65))
  val dataFrame = sparkSession.createDataFrame(userData).toDF("name", "age", "score")

  def main(args: Array[String]): Unit = {

    dataFrame
      .filter("name is not null")
      .orderBy("age", "score")
//      .show()

    val cnt = dataFrame
      .filter(dataFrame("name") contains "ac").count
    println(cnt)

      dataFrame.filter(not(dataFrame("name") like "%ar%")).collect.foreach(println(_))

    dataFrame.filter(dataFrame("name") like "%ar%").collect.foreach(println(_))

    println("-----")
    //LIKE with Regex
    dataFrame.filter(dataFrame("name") rlike ".*e.*").collect.foreach(println(_))

  }

}
