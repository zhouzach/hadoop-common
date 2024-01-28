import org.apache.spark.sql.SparkSession

object Q4 {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold", 104857600)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .enableHiveSupport()
    .getOrCreate()


  def main(args: Array[String]): Unit = {


    sparkSession.sql(
      """
        |
        |CREATE  TABLE IF NOT EXISTS t_login_with_partition1 (
        |    logtime string,
        |    account_id bigint,
        |    province string
        |)
        |PARTITIONED BY (log_date string)
        |STORED AS parquet
        |
        |""".stripMargin)


    val ip_data = "/Users/Zach/hadoop-common/src/main/resources/ip_china.csv"


    val ipDF = sparkSession.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED") //ignores the whole corrupted records
      .load(ip_data)

    val login_data = "/Users/Zach/hadoop-common/src/main/resources/login_data.csv"
    val loginDF = sparkSession.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED") //ignores the whole corrupted records
      .load(login_data)

    import org.apache.spark.sql.functions.broadcast
    ipDF.join(
      broadcast(loginDF)
    ).where(loginDF("ip") > ipDF("ip_start") && loginDF("ip") < ipDF("ip_end"))
      .select("logtime", "account_id", "country", "province")
      .createOrReplaceTempView("t_login_data")


    sparkSession.sql(
      """
        |
        |insert overwrite table t_login_with_partition1
        |partition(log_date)
        |select logtime,account_id,province,substr(logtime,1,10) as log_date
        |from t_login_data
        |""".stripMargin)

    sparkSession.sql(
      """
        |
        |select * from t_login_with_partition1
        |""".stripMargin)
      .groupBy("log_date", "province", "account_id")
      .count()
      .groupBy("log_date", "province")
      .count()
      .withColumnRenamed("log_date", "pt")
      .withColumnRenamed("count", "cnt_login")
      .show(100, false)


  }

}
