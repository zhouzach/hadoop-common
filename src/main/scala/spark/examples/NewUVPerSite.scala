package spark.examples

import org.apache.spark.sql.SparkSession

object NewUVPerSite {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("NewUVDemo").master("local").getOrCreate()
    val rdd1 = spark.sparkContext.parallelize(
      Array(
        ("2017-01-01", "a", "www.sina.com"), ("2017-01-01", "a", "www.sina.com"), ("2017-01-01", "a", "www.sina.com"),
        ("2017-01-01", "b", "www.sina.com"), ("2017-01-03", "e", "www.sina.com"), ("2017-01-02", "b", "www.sina.com"),

        ("2017-01-01", "c", "www.qq.com"), ("2017-01-01", "d", "www.qq.com"), ("2017-01-02", "d", "www.qq.com"),
        ("2017-01-03", "f", "www.qq.com"),

        ("2017-01-02", "a", "www.baidu.com"),


        ("2017-01-03", "b", "www.163.com"), ("2017-01-03", "b", "www.163.com")
      ))


    val rdd2 = rdd1.map(kv => (kv._3, (kv._2, kv._1)))
    val siteRdd = rdd2.groupByKey()

    siteRdd
      .collect() // can print after collect
      .map(r => {
      val site = r._1

      val data = r._2
        .groupBy(_._1).map(r2 => (r2._1, r2._2.map(_._2).min))  //倒排索引
        .groupBy(_._2).map(r3 => (r3._1, r3._2.size))   //countByKey

      (site, data)
    }).foreach(println(_))
  }
}

