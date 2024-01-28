import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{first, row_number}


object Q5 {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", 104857600)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()


  def main(args: Array[String]): Unit = {


    //    sparkSession.sql(
    //      """
    //        |
    //        |CREATE  TABLE IF NOT EXISTS t_login_with_partition2 (
    //        |    logtime string,
    //        |    account_id bigint,
    //        |    province string
    //        |)
    //        |PARTITIONED BY (log_date string)
    //        |STORED AS parquet
    //        |
    //        |""".stripMargin)
    //
    //
    //    val ip_data = "/Users/Zach/hadoop-common/src/main/resources/ip_china.csv"
    //    val ipDF = sparkSession.read
    //      .format("csv")
    //      .option("header", "true") //first line in file has headers
    //      .option("mode", "DROPMALFORMED") //ignores the whole corrupted records
    //      .load(ip_data)
    //
    //    val login_data = "/Users/Zach/hadoop-common/src/main/resources/login_data.csv"
    //    val loginDF = sparkSession.read
    //      .format("csv")
    //      .option("header", "true") //first line in file has headers
    //      .option("mode", "DROPMALFORMED") //ignores the whole corrupted records
    //      .load(login_data)
    //
    //    import org.apache.spark.sql.functions.broadcast
    //    ipDF.join(
    //      broadcast(loginDF)
    //    ).where(loginDF("ip") > ipDF("ip_start") && loginDF("ip") < ipDF("ip_end"))
    //      .select("logtime", "account_id", "country", "province")
    //      .createOrReplaceTempView("t_login_data")
    //    sparkSession.sql(
    //      """
    //        |
    //        |insert overwrite table t_login_with_partition2
    //        |partition(log_date)
    //        |select logtime,account_id,province,substr(logtime,1,10) as log_date
    //        |from t_login_data
    //        |""".stripMargin)

    val windowSpec = Window.partitionBy("province").orderBy("logtime")

    val loginPartitionDF = sparkSession.sql(
      """
        |
        |select * from t_login_with_partition2
        |""".stripMargin)
    loginPartitionDF
      .groupBy("province", "account_id", "logtime")
      .count()
      .withColumn("row_number", row_number.over(windowSpec))
      .where("row_number < 4")
      .groupBy("province")
      .pivot("row_number")
      .agg(first("account_id", true).as("account_id"), first("logtime", true).as("logtime"))
      .withColumnRenamed("1_account_id", "account_id_1")
      .withColumnRenamed("1_logtime", "login_time_1")
      .withColumnRenamed("2_account_id", "account_id_2")
      .withColumnRenamed("2_logtime", "login_time_2")
      .withColumnRenamed("3_account_id", "account_id_3")
      .withColumnRenamed("3_logtime", "login_time_3")
      .show(100, false)


  }

}
