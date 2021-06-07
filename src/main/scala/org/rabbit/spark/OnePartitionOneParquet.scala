package org.rabbit.spark

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object OnePartitionOneParquet {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //加上这个设置,要不然所有的overwrite了
    .config("org.rabbit.spark.sql.sources.partitionOverwriteMode", "dynamic")
    .getOrCreate()

  def merge(dataFrame: DataFrame, location: String) = {
    import sparkSession.implicits._

    dataFrame
      .repartition($"entity", $"year", $"month", $"day", $"status")
      //      .coalesce(200)
      .write
      //      .partitionBy("dt")
      .partitionBy("entity", "year", "month", "day", "status")
      .mode(SaveMode.Append).parquet(s"$location")
  }

}
