package spark.ml

import org.apache.spark.sql.SparkSession

object StringIndexerPipeline {

  val spark: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .getOrCreate()


  def main(args: Array[String]): Unit = {

  }

}
