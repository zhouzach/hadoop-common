package org.rabbit.spark.writeTo

import org.apache.spark.sql.{SaveMode, SparkSession}

object Jdbc {
  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //加上这个设置,要不然所有的overwrite了
    .config("org.rabbit.spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val userData = Seq(("1","2","3"))
    sparkSession.createDataFrame(userData).toDF("a", "b","c")
      .write
      .mode(SaveMode.Append)
      //            .format("jdbc")
      //            .option("url","jdbc:mysql://172.19.245.111:3306/banban?useUnicode=true&characterEncoding=utf-8&allowMultiQueries=true&autoReconnect=true")
      //            .option("user","banban")
      //            .option("password","banban123456")
      //            .option("driver","com.mysql.cj.jdbc.Driver")
      //            .option("dbtable","dw.analysis_gift_consume3")
      //            .save()
      .jdbc(DbConfig.url,"dw.analysis_gift_consume4",DbConfig.pop)
  }

}
