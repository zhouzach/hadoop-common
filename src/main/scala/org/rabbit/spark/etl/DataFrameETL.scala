package org.rabbit.spark.etl

import java.util.Properties

import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.rabbit.config.FileConfig
import org.slf4j.{Logger, LoggerFactory}

/**
 * https://bigdata-etl.com/in-what-way-effectively-exploiting-api-dataframe-while-loading-data/
 */
object DataFrameETL {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val orders = Seq(
      ("o1", "u1", "2019-07-01", 100.0),
      ("o2", "u1", "2019-07-22", 120.0),
      ("o3", "u2", "2019-08-01", 100.0),
      ("o4", "u1", "2019-08-02", 220.0),


      ("o5", "u2", "2019-09-12", 220.0)
    )
    //    val orderDF = sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "order_time", "price")
    //    export2csv(orderDF, "/Users/Zach/org.rabbit.hadoop-common/output")

    //    val path = "hdfs://nameservice1/data/ods/banban/user_base_info_00/part-m-00000"
    //    val schemaPath = "file:////Users/Zach/org.rabbit.hadoop-common/src/main/resources/user.schema"
    //    loadCSVBySql(path)
    val path = "hdfs://nameservice1/apps/operation/anchor_no_live_phones.txt"
    loadCSV(path)
    //    loadCSV(path,schemaPath,"|","")

  }

  def exportByJDBC(dataFrame: DataFrame, oracleTableName: String) = {

    val oracleConfig = FileConfig.oracleConfig
    val oracleUrl = oracleConfig.getString("url")
    val oracleDriver = oracleConfig.getString("driver")
    val oracleUsername = oracleConfig.getString("username")
    val oraclePassword = oracleConfig.getString("password")

    val prop = new Properties();
    //    prop.setProperty("database", "localhost");
    prop.setProperty("user", oracleUsername);
    prop.setProperty("password", oraclePassword);

    dataFrame.write.mode(SaveMode.Append)
      .jdbc(oracleUrl, oracleTableName, prop)
  }

  def export2parquet(dataFrame: DataFrame, fileFullPath: String) = {
    dataFrame.write.mode(SaveMode.Overwrite).parquet(fileFullPath)
  }

  def export2csv(dataFrame: DataFrame, fileFullPath: String) = {
    dataFrame.write
      .option("header", "true")
      .option("inferSchema", "true")
      .option("delimiter", "|")
      .mode(SaveMode.Overwrite).csv(fileFullPath)
  }


  def loadByJDBC(oracleTable: String, sparkTable: String) = {

    val oracleConfig = FileConfig.oracleConfig
    val oracleUrl = oracleConfig.getString("url")
    val oracleDriver = oracleConfig.getString("driver")
    val oracleUsername = oracleConfig.getString("username")
    val oraclePassword = oracleConfig.getString("password")

    val tmpTable = sparkTable + "_TMP"
    sparkSession.sql(s"drop table if exists $tmpTable")
    sparkSession.sql(
      s"""
         |
         |CREATE  external TABLE $tmpTable USING jdbc OPTIONS(
         | dbtable '$oracleTable',
         | driver '$oracleDriver',
         | user '$oracleUsername',
         | password '$oraclePassword',
         | url '$oracleUrl')
         |
       """.stripMargin
    )

    sparkSession.sql(
      s"""
         |
         |INSERT INTO $sparkTable SELECT * from $tmpTable
         |
       """.stripMargin)
  }

  def loadCSV(csvPath: String, schemaPath: String, delimiter: String, sparkTable: String) = {

    //./src/main/resources/people.schema
    val ddl = sparkSession.sparkContext.textFile(schemaPath)
      .toLocalIterator.toList.mkString("")
    val schema = StructType.fromDDL(ddl)
    //    val schema = sparkSession.table(s"$sparkTable").schema

    val confirmDetailDF = sparkSession.read
      .schema(schema)
      .format("csv")
      .option("header", "false")
      //          .option("mode", "DROPMALFORMED")
      .option("delimiter", delimiter)
      .load(csvPath)
      .cache()

    confirmDetailDF.select("uid", "nickname").show(100)

    //    confirmDetailDF.write.insertInto(s"$sparkTable")
  }

  def loadCSV(path: String) = {
    val df = sparkSession.read
      .format("csv")
      //      .option("header", "true") //first line in file has headers
      /**
       * https://spark.apache.org/docs/2.0.2/api/java/org/apache/spark/sql/DataFrameReader.html
       * PERMISSIVE : sets other fields to null when it meets a corrupted record. When a schema is set by user, it sets null for extra fields.
       * DROPMALFORMED : ignores the whole corrupted records.
       * FAILFAST : throws an exception when it meets corrupted records.
       */
      .option("mode", "DROPMALFORMED")
      .load(path)
//      .filter(!_.isNullAt(0)) // load时，已经过滤空行

    println(
      s"""
         |
         |count: ${df.count()}
         |""".stripMargin)
    df.show(500)
  }

  def loadCSVBySql(path: String): Unit = {
    sparkSession.sql(
      s"""
         |
         |select * from csv.`$path`
         |
      """.stripMargin)
      .show(100)
  }

  def loadParquetBySql(): Unit = {
    sparkSession.sql(
      """
        |
        |select * from parquet.`hdfs://path`
        |
      """.stripMargin)
  }
}
