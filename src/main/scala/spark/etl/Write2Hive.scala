package spark.etl

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object Write2Hive {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") //只覆盖对应分区的功能
    //    .enableHiveSupport()
    .getOrCreate()


  def main(args: Array[String]): Unit = {

  }


  //https://xieyuanpeng.com/2019/04/07/spark-to-hive/
  def export2parquet(dataFrame: DataFrame, fileFullPath: String) = {
    dataFrame.write.mode("overwrite").insertInto("partitioned_table")

    //直接写文件到 Hive 对应的 HDFS 路径
    dataFrame.write.mode(SaveMode.Overwrite).save("/root/path/to/data/partition_col=value")
  }

}
