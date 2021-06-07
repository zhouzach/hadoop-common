package org.rabbit.spark.rdd

import org.apache.spark.TaskContext
import org.apache.spark.sql.SparkSession

object CombineByKey {
  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val data=List(("A",1),("B",3),("A",2),("B",1),("A",3),("B",2),("C",1))
    val rdd=sparkSession.sparkContext.parallelize(data, 2)
    rdd
      .foreach(row => {
        println("partitionId：" + TaskContext.get.partitionId+ ", "+row)
      })

    /**
     * partitionId：0, (A,1)
     * partitionId：0, (B,3)
     * partitionId：0, (A,2)
     *
     * partitionId：1, (B,1)
     * partitionId：1, (A,3)
     * partitionId：1, (B,2)
     * partitionId：1, (C,1)
     */

    rdd.combineByKey(
      (v : Int) => v + "_",                          //key在分区中第一次出现，对其value应用
      (c : String, v : Int) => c + "@" + v,          //key在分区中不是第一次出现时，对其value应用
      (c1 : String, c2 : String) => c1 + "$" + c2)   //合并不同分区相同key的累加value结果
      .collect()
      .foreach(println(_))

    /**
     * (B,3_$1_@2)
     * (A,1_@2$3_)
     * (C,1_)
     */
  }

}
