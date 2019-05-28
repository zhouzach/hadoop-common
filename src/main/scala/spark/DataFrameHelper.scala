package spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object DataFrameHelper {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .enableHiveSupport()
    .getOrCreate()

  val dataFrame: DataFrame = sparkSession.sql("")

  def createDataFrameBySeq() = {
    val userData = Seq(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    sparkSession.createDataFrame(userData).toDF("name", "age")
  }

  /**
    * https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393
    */
  def createDataFrame() = {
    val someData = Seq(
      Row(8, "bat"),
      Row(64, "mouse"),
      Row(-27, "horse")
    )

    val someSchema = List(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true)
    )

    sparkSession.createDataFrame(
      sparkSession.sparkContext.parallelize(someData),
      StructType(someSchema)
    )

  }

  def createDataFrame(sparkSession: SparkSession) = {

    import sparkSession.implicits._

    Seq(
      ("a", "b", 1),
      ("a", "b", 2),
      ("a", "b", 3),
      ("z", "b", 4),
      ("a", "x", 5)
    ).toDF("letter1", "letter2", "number1")
  }

  def transform(dataFrame: DataFrame) = {
    sparkSession.createDataFrame(dataFrame.rdd, dataFrame.schema)
  }

  def dataFrame2Table(dataFrame: DataFrame) = {
    dataFrame.createOrReplaceTempView("people")
    sparkSession.sql("select name from people").collect.foreach(println)
  }

  def dataFrame2Array(dataFrame: DataFrame) = {
    dataFrame.collect().map { row =>
      val segmentId = row.getAs[Int]("segment_id")
      val user_count = row.getAs[Long]("user_count")
      println(s"segmentId: $segmentId")
      println(s"audience_group_user_count: $user_count")

      (segmentId, user_count)
    }
  }

  def dataFrame2RDD(dataFrame: DataFrame) = {
    dataFrame.rdd
  }

  def dataFrameFromRDD() = {

    val schema =
      StructType(
        StructField("name", StringType, false) ::
          StructField("age", IntegerType, true) :: Nil)

    val peopleRDD =
      sparkSession.sparkContext.textFile("examples/src/main/resources/people.txt").map(
        _.split(",")).map(p => Row(p(0), p(1).trim.toInt))

    val dataFrame = sparkSession.createDataFrame(peopleRDD, schema)

    dataFrame.printSchema
    // root
    // |-- name: string (nullable = false)
    // |-- age: integer (nullable = true)

    dataFrame.createOrReplaceTempView("people")
    sparkSession.sql("select name from people").collect.foreach(println)
  }

}
