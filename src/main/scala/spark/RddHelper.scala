package spark

import org.apache.spark.sql.SparkSession

object RddHelper {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()


  val fileRdd = sparkSession.sparkContext.textFile("examples/src/main/resources/people.txt")
      .toLocalIterator.toList(0)

  val numRdd =sparkSession.sparkContext.parallelize(Seq(1,2,3))

}
