import org.apache.spark.sql.{SaveMode, SparkSession}

object Q3 {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .config("spark.sql.autoBroadcastJoinThreshold",104857600)
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
    .config("spark.sql.parquet.compression.codec","snappy")
    .enableHiveSupport()
    .getOrCreate()


  def main(args: Array[String]): Unit = {



    val ip_data = "/Users/Zach/hadoop-common/src/main/resources/ip_china.csv"

    val ipDF = sparkSession.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED") //ignores the whole corrupted records
      .load(ip_data)
//    ipDF.show(12)

    val login_data = "/Users/Zach/hadoop-common/src/main/resources/login_data.csv"
    val loginDF = sparkSession.read
      .format("csv")
      .option("header", "true") //first line in file has headers
      .option("mode", "DROPMALFORMED") //ignores the whole corrupted records
      .load(login_data)
//    loginDF.show(12)

    import org.apache.spark.sql.functions.broadcast
    ipDF.join(
      broadcast(loginDF)
    ).where(loginDF("ip")>ipDF("ip_start") && loginDF("ip")<ipDF("ip_end"))
      .select("logtime","account_id","country","province")
            .show(10)
//      .write.mode(SaveMode.Overwrite).insertInto("t_login_with_province")
  }

}
