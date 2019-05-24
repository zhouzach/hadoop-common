package spark

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * https://www.mungingdata.com/apache-spark/deduplicating-and-collapsing
  */
object DeduplicatingAndCollapsing {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .enableHiveSupport()
    .getOrCreate()

  val dataFrame: DataFrame = sparkSession.sql("")

  def createDataFrame(sparkSession: SparkSession) = {

    import sparkSession.implicits._

    val df = Seq(
      ("a", "b", 1),
      ("a", "b", 2),
      ("a", "b", 3),
      ("z", "b", 4),
      ("a", "x", 5)
    ).toDF("letter1", "letter2", "number1")


//    df
//      .groupBy("letter1", "letter2")
//      .agg(max("age"), sum("expense"))
//      .show()
  }


}
