package org.rabbit.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object SparkSqlOptimizer {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()


  /**
    * https://confusedcoders.com/data-engineering/spark/spark-sql-job-executing-very-slow-performance-tuning
    */
  def handleSlowQuery(dataFrame: DataFrame): Unit ={
    // For handling large number of smaller files
    val events = sparkSession.sqlContext.createDataFrame(dataFrame.rdd, dataFrame.schema).coalesce(400)
    events.createOrReplaceTempView("input_events")
    // For overriding default value of 200
    sparkSession.sqlContext.sql("SET org.rabbit.spark.sql.shuffle.partitions=10")

    val sql_query =""
    sparkSession.sqlContext.sql(sql_query)
  }

}
