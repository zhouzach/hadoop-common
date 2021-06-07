package org.rabbit.spark

import org.apache.spark.sql.SparkSession

object UDFHelper {
  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
//    .enableHiveSupport()
    .getOrCreate()

  def execUdf() {

    // 构造测试数据，有两个字段、名字和年龄
    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))

    //创建测试df
    val userDF = sparkSession.createDataFrame(userData).toDF("name", "age")
    import org.apache.spark.sql.functions._
    //注册自定义函数（通过匿名函数）
    val strLen = udf((str: String) => str.length())
    //注册自定义函数（通过实名函数）
    val udf_isAdult = udf(isAdult _)

    //通过withColumn添加列
    userDF.withColumn("name_len", strLen(col("name"))).withColumn("isAdult", udf_isAdult(col("age"))).show
    //通过select添加列
    userDF.select(col("*"), strLen(col("name")) as "name_len", udf_isAdult(col("age")) as "isAdult").show

    //关闭
//    sparkSession.stop()
  }

  /**
    * 根据年龄大小返回是否成年 成年：true,未成年：false
    */
  def isAdult(age: Int) = {
    if (age < 18) {
      false
    } else {
      true
    }

  }

  def main(args: Array[String]): Unit = {
    execUdf()
  }


}
