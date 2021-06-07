package org.rabbit.spark

import org.apache.spark.sql.Encoders
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object StructTypeHelper {


  def main(args: Array[String]): Unit = {

    println(fromDDL())

    fromClass()
//      .foreach(println(_))
  }

  def getField() = {
    val struct = fromSeq()
    val word = struct("word")
    println(word)
  }

  def fromSeq() = {
    val fields = Seq(
      StructField("number", IntegerType, true),
      StructField("word", StringType, true)
    )
    StructType(fields)
  }

  def fromDDL() = {
    val ddl =
      """
        |`id` BIGINT,`name` STRING
        |""".stripMargin

    StructType.fromDDL(ddl)
    StructType.fromDDL ("id integer, name string, surname string, age integer")
  }

  def fromClass() = {
    Encoders.product[Person].schema
  }

  //@since 2.4.0
//  def toDDL() = {
//    fromDDL().toDDL
//  }
}


case class Person(id: Long, name: String)
