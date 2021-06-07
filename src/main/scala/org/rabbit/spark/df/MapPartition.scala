package org.rabbit.spark.df

import org.apache.spark.sql.SparkSession

object MapPartition {
  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {
    import sparkSession.implicits._
    val df = Seq(1, 2, 3).toDF("ids")

    df.mapPartitions(iter => {
      var res = Seq[(Int, Int)]()

      while (iter.hasNext) {
        val cur = iter.next.getAs[Int](0);
        res = res.+: (cur, cur * cur)
      }

      res.iterator
    }).show()
  }

}
