package org.rabbit

import org.apache.spark.sql.{Encoders, SparkSession}

package object spark {
  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")

    .master("local")
    .config("spark.sql.crossJoin.enabled",true)
    //    .enableHiveSupport()
    .getOrCreate()

  import sparkSession.sqlContext.implicits._

  implicit val personEncoder = Encoders.product[(String, Seq[(String, String, Seq[String])])]

}
