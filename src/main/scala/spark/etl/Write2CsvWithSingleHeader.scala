package spark.etl

import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import collection.JavaConverters._
import org.apache.spark.sql.{Row, SaveMode}
import spark.sql.CaseWhen.sparkSession

/**
 * https://stackoverflow.com/questions/38056152/merge-spark-output-csv-files-with-a-single-header/42913696#42913696
 */
object Write2CsvWithSingleHeader {


  def main(args: Array[String]): Unit = {


    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0, 1),
      ("o2", "u1", "2019-07-22", 120.0, 2),
      ("o4", "u1", "2019-08-02", 220.0, -4)
    )
    val orderDF = sparkSession.createDataFrame(orders)
      .toDF("order_id", "member_id", "order_time", "price", "qty")

    //cast types of all columns to String, because the type of headerDF columns are string
    val dataStrDF = orderDF.select(orderDF.columns.map(c => orderDF.col(c).cast("string")): _*)

    val headerDF = sparkSession.createDataFrame(List(Row.fromSeq(dataStrDF.columns)).asJava, dataStrDF.schema)

    //merge header names with data
    headerDF.union(dataStrDF).write.mode(SaveMode.Overwrite)
      .option("header", "false").csv("/Users/Zach/hadoop-common/output")

    //use hadoop FileUtil to merge all partition csv files into a single file
    val fs = FileSystem.get(sparkSession.sparkContext.hadoopConfiguration)
    FileUtil.copyMerge(fs, new Path("/Users/Zach/hadoop-common/output"),
      fs, new Path("/Users/Zach/hadoop-common/target.csv"),
      true, sparkSession.sparkContext.hadoopConfiguration, null)


  }

}
