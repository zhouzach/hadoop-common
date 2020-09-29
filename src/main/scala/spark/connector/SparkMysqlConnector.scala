package spark.connector

import java.sql.DriverManager

import org.apache.spark.sql.SparkSession

//https://docs.databricks.com/data/data-sources/sql-databases.html
object SparkMysqlConnector {
  val spark = SparkSession
    .builder()
    .appName("myApp")
    .master("local[*]")
    .getOrCreate()

  val jdbcHostname = "10.0.6.93"
  val jdbcPort = 3306
  val jdbcDatabase = "banban"

  // Create the JDBC URL without passing in the user and password parameters.
  val jdbcUrl = s"jdbc:mysql://${jdbcHostname}:${jdbcPort}/${jdbcDatabase}"

  // Create a Properties() object to hold the parameters.

  import java.util.Properties

  val connectionProperties = new Properties()

  val jdbcUsername = "zzq"
  val jdbcPassword = "123456"
  connectionProperties.put("user", s"${jdbcUsername}")
  connectionProperties.put("password", s"${jdbcPassword}")


  def main(args: Array[String]): Unit = {
    //    checkConnectivity()
//    readFromMysql()
    readFromMysqlByPushdownQuery()

  }

  // Check connectivity to the MySQL database
  def checkConnectivity() = {
    Class.forName("com.mysql.jdbc.Driver")

    val connection = DriverManager.getConnection(jdbcUrl, jdbcUsername, jdbcPassword)
    println(connection)
    connection.isClosed()
  }

  def readFromMysqlByPushdownQuery() = {
    // Note: The parentheses are required.
    val pushdown_query = "(select * from bill_202009 where id > 34360) bill"
    spark.read.jdbc(url = jdbcUrl, table = pushdown_query, properties = connectionProperties)
      .show()
  }


  def readFromMysql() = {
    val employees_table = spark.read.jdbc(jdbcUrl, "employees", connectionProperties)

    // Explain plan with no column selection returns all columns
    spark.read.jdbc(jdbcUrl, "bill_202009", connectionProperties)
      .explain(true)

    // Explain plan with column selection will prune columns and just return the ones specified
    // Notice that only the 3 specified columns are in the explain plan
    spark.read.jdbc(jdbcUrl, "bill_202009", connectionProperties).select("gid", "srcuid", "dstuid")
      .explain(true)

    // You can push query predicates down too
    // Notice the filter at the top of the physical plan
    spark.read.jdbc(jdbcUrl, "bill_202009", connectionProperties).select("gid", "srcuid", "dstuid")
      .where("dstnickname = '张6'").explain(true)
  }

}
