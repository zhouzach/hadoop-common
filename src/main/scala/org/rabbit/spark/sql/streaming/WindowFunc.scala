package org.rabbit.spark.sql.streaming

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.window

object WindowFunc {

  val sparkSession: SparkSession = SparkSession.builder.master("local").getOrCreate()

  def main(args: Array[String]): Unit = {

    val kafkaDF = sparkSession.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "192.168.1.237:9092")
      //.option("subscribe", "hudikafkatest")
      .option("subscribePattern", "hudikafka.*")
      //.option("startingOffsets", "latest")
      .option("kafka.group.id", "g1")
      .option("startingOffsets", "earliest")
      .option("maxOffsetsPerTrigger", 100000)
      .load()

    import sparkSession.implicits._


    kafkaDF
      .withWatermark("timestamp", "10 minutes")

      .groupBy(
      window($"timestamp", "10 minutes", "5 minutes"),
      $"word"
    ).count()

  }

}
