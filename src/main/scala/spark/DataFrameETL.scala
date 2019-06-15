package spark

import java.util.Properties

import config.FileConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.slf4j.{Logger, LoggerFactory}

object DataFrameETL {

  val logger: Logger = LoggerFactory.getLogger(getClass)

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    .enableHiveSupport()
    .getOrCreate()

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

  def export2Hdfs(dataFrame: DataFrame, fileFullPath: String) = {
    dataFrame.write.mode(SaveMode.Overwrite).parquet(fileFullPath)
  }


  def loadByJDBC(oracleTable: String, sparkTable: String)={

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

  def loadCSV(fileFullPath: String, delimiter: String, sparkTable: String) = {

    val schema = sparkSession.table(s"$sparkTable").schema

    //        = new StructType()
    //          .add("begin", StringType, true)
    //          .add("end", StringType, true)
    //          .add("TASerialNO", StringType, true)
    //          .add("SerialNO", StringType, true)
    //          .add("clarity", StringType, true)
    //          .add("depth", DoubleType, true)
    //          .add("table", DoubleType, true)
    //          .add("price", IntegerType, true)
    //          .add("x", DoubleType, true)
    //          .add("y", DoubleType, true)
    //          .add("z", DoubleType, true)

    val confirmDetailDF = sparkSession.read
      .schema(schema)
      .format("csv")
      .option("header", "false")
      .option("inferSchema", "true")
      //          .option("mode", "DROPMALFORMED")
      .option("delimiter", delimiter)
      .load(fileFullPath)
      .cache()

    confirmDetailDF.show()

    val dataCount = confirmDetailDF.count()

    confirmDetailDF.write.insertInto(s"$sparkTable")


    val count = sparkSession.sql(s"select count(*) from $sparkTable").collect().head.getAs[Long](0)
    logger.info(s"insert $count records into table $sparkSession")

  }
}
