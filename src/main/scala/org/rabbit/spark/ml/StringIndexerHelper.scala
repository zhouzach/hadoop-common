package org.rabbit.spark.ml

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

object StringIndexerHelper {

  val spark: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val training = spark.createDataFrame(
      Seq((0, "a"), (1, "b"), (2, "c"), (3, "a"), (4, "a"), (5, "c"))
    ).toDF("id", "category")

    val test = spark.createDataFrame(
      Seq((0, "a", "x"), (1, "b", "y"), (2, "c", "z"), (3, "a","x"), (4, "a", "x"), (5, "c","z"))
    ).toDF("id", "category", "value")

    val indexer = new StringIndexer()
      .setInputCol("category")
      .setOutputCol("categoryIndex")
      .setHandleInvalid("skip")

    val indexed = indexer.fit(training)
      .transform(test)
    indexed.show()
  }

}
