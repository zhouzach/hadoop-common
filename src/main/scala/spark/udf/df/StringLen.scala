package spark.udf.df

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object StringLen {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {


    val strLen = udf((str: String) => str.length())

    val udf_isAdult = udf(isAdult _)

    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    val userDF = sparkSession.createDataFrame(userData).toDF("name", "age")
    userDF.select(col("*"), strLen(col("name")) as "name_len", udf_isAdult(col("age")) as "isAdult").show

  }

  def isAdult(age: Int) = {
    if (age < 18) {
      false
    } else {
      true
    }

  }


}
