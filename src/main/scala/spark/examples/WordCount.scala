package spark.examples

import org.apache.spark.sql.SparkSession

object WordCount {
  val spark = SparkSession.builder().appName("NewUVDemo").master("local").getOrCreate()

  def main(args: Array[String]): Unit = {

    val textFile = spark.sparkContext.textFile("words.txt")
    val counts = textFile.flatMap(line => line.split(" "))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
    counts.collect().foreach(wc => println(wc._1+ ":" + wc._2))
//    counts.saveAsTextFile("hdfs://...")
  }

}
