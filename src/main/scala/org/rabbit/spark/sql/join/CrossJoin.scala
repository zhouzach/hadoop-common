package org.rabbit.spark.sql.join

import org.rabbit.spark.sparkSession

object CrossJoin {

  def main(args: Array[String]): Unit = {

//    sparkSession.sql(
//      """
//        |--- spark.sql.crossJoin.enabled=true
//        |select  *
//        |from (select 3 as num1) t1 join (select 2 as num2) t2
//        |""".stripMargin)
//      .show()


//        sparkSession.sql(
//          """
//            |with t1 as (
//            | select 3 as num1
//            |),
//            |t2 as (
//            | select 2 as num2
//            |)
//            |
//            |select  *
//            |from t1 full join  t2 on t1.num1 != t2.num2
//            |""".stripMargin)
//          .show()

    sparkSession.sql(
      """
        |with t1 as (
        | select 3 as num1
        |),
        |t2 as (
        | select 2 as num2
        |)
        |
        |select  *
        |from t1 full join  t2
        |""".stripMargin)
      .show()
  }

}
