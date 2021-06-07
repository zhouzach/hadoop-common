package org.rabbit.spark

import org.apache.spark.sql.Row

object RowHelper {

  def main(args: Array[String]): Unit = {
    val s1 = Seq(1, 2, 3)
    val s2 = Seq("a", "b", "c")
    println(fromSeq(s1))
    println(fromTuple(("a", 1)))
  }

  def fromSeq[T](seq: Seq[T]) = {
    Row.fromSeq(seq)
  }

  def fromTuple(t: (String, Int)) = {
    Row.fromTuple(t)
  }



}
