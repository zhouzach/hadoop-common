import org.apache.spark.sql.SparkSession

object SparkDataHelper {

  val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

  def dataFrame2Object(): Unit ={
    spark.sql("").collect().foreach { row =>
      val segmentId = row.getAs[Int]("segment_id")
      val user_count = row.getAs[Long]("user_count")
      println(s"segmentId: $segmentId")
      println(s"audience_group_user_count: $user_count")

    }

  }

}
