package spark.connector

import com.redislabs.provider.redis._
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkRedisConnector {

  val spark = SparkSession
    .builder()
    .appName("myApp")
    .master("local[*]")
    //    .config("spark.redis.host", "172.19.22.136")
    //    .config("spark.redis.port", "6379")
    //    .config("spark.redis.auth", "qile123")
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {

    val redisServerDnsAddress = "172.19.22.136"
    val redisPortNumber = 6379
    val redisPassword = "qile123"
    val redisConfig = new RedisConfig(RedisEndpoint(redisServerDnsAddress, redisPortNumber, redisPassword))

    val lives = Seq(
      ("o1", "u3", "m1", 1599100484, 100.0), //2020/9/3 10:34:44
      ("o2", "u3", "m2", 1599014084, 120.0), //2020/9/2 10:34:44
      ("o3", "u3", "m2", 1598927684, 120.0), //2020/9/1 10:34:44
      ("o4", "u3", "m2", 1598931284, 120.0), //2020/9/1 11:34:44
      ("o5", "u3", "m2", 1598924084, 120.0), //2020/9/1 9:34:44
      ("o6", "u4", "m2", 1599100484, 100.0) //2020/9/3 10:34:44
    )
    spark.createDataFrame(lives).toDF("order_id", "member_id", "m_code", "order_time", "price")
      .createOrReplaceTempView("lives")

    val df = spark.sql(
      """
        |
        |with d as (
        |   select member_id, from_unixtime(order_time, '%Y-%m-%d') as day
        |from lives
        |group by member_id,from_unixtime(order_time, '%Y-%m-%d')
        |)
        |
        |select member_id, count(day) as days
        |from d
        |group by member_id
        |
          """.stripMargin)
//    df.show()
    //      .write
    //      .format("org.apache.spark.sql.redis")
    //      .option("table", "foo")
    //      .save()
    //      .show()

//    writeZSet(df,redisConfig)

        readZSetWithScore(sc,redisConfig)
  }

  def readZSetWithScore(sc: SparkContext, redisConfig: RedisConfig) = {
    sc.fromRedisZSetWithScore("setkey*")(redisConfig)
      .collect().foreach(println(_))
  }

  def writeZSet(df: DataFrame, redisConfig: RedisConfig) = {
    import spark.implicits._
    val kvs = df.map(r => (r.getAs[String](0), r.getAs[Long](1).toString))
    sc.toRedisZSET(kvs.rdd, "setkey3")(redisConfig)
  }


}
