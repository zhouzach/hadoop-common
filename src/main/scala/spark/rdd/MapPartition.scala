package spark.rdd

import org.apache.spark.sql.SparkSession

object MapPartition {
  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    val rdd = sparkSession.sparkContext.makeRDD(1 to 5,2)

    rdd.mapPartitions(iter => {
      var res = List[(Int, Int)]()

      while (iter.hasNext) {
        val cur = iter.next;
        res.::=(cur, cur * cur)
      }

      res.iterator
    }).collect().foreach(println(_))

  }

}
