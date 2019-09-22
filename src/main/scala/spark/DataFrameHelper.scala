package spark

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import scala.collection.JavaConverters._


object DataFrameHelper {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()


  def main(args: Array[String]): Unit = {
    createDataFrameFromSeqSchema().show()
  }

  def createEmptyDataFrame() = {
    sparkSession.createDataFrame(sparkSession.sparkContext.emptyRDD[Row], sparkSession.table("").schema)
  }

  def createDataFrameFromSeq() = {
    import sparkSession.implicits._
    Seq(1,2,3).toDF("ids").show()
  }

  def createDataFrameFromSeqSchema()  = {
    val fields = Seq(
      StructField("col1", IntegerType, false),
      StructField("col2", IntegerType, false),
      StructField("col3", IntegerType, false)
    )
    val schema = StructType(fields)
    sparkSession.createDataFrame(List(Row.fromSeq(Seq(1,2,3))).asJava, schema)
  }

  def createDataFrameTupleSeq() = {
    val userData = Seq(("1","2","3"))
//    val userData = Seq(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
//    sparkSession.createDataFrame(userData).toDF("name", "age")
    sparkSession.createDataFrame(userData).toDF("a", "b","c")
  }

  def dataFrameFromRDD() = {
    val rdd =sparkSession.sparkContext.parallelize(Seq(1,2,3))

    import sparkSession.sqlContext.implicits._
    rdd.toDF("id")
  }


  /**
    * https://medium.com/@mrpowers/manually-creating-spark-dataframes-b14dae906393
    */
  def createDataFrameFromRddSchema() = {
    val someData = Seq(
      Row(8, "bat"),
      Row(64, "mouse"),
      Row(-27, "horse")
    )
    val rdd = sparkSession.sparkContext.parallelize(someData)

    val someSchema = List(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true)
    )

    sparkSession.createDataFrame(
      rdd,
      StructType(someSchema)
    )

  }


  def createDataFrameFromDataFrame(dataFrame: DataFrame) = {
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

}
