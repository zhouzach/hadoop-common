package org.rabbit.config

import org.apache.spark.sql.SparkSession

object SparkConfig {
  val spark: SparkSession = SparkSession
    .builder()
    // hoodie only support org.apache.spark.serializer.KryoSerializer as spark.serializer
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

    .config("org.rabbit.spark.sql.crossJoin.enabled", "true")
    .config("org.rabbit.spark.debug.maxToStringFields", 100)
    .config("org.rabbit.spark.sql.broadcastTimeout", 36000)
    .config("org.rabbit.spark.driver.maxResultSize", "30g")
    .enableHiveSupport()
    .master("local")
    .getOrCreate()


}
