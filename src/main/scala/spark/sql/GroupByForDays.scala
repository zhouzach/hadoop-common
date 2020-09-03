package spark.sql

import spark.sparkSession

object GroupByForDays {


  def main(args: Array[String]): Unit = {



    val orders = Seq(
      ("o1", "u1", "m1","2019-07-11", 100.0),
      ("o2", "u1", "m2","2019-07-02", 120.0),
      ("o3", "u2", "m2","2019-06-01", 100.0)

    )
    sparkSession.createDataFrame(orders).toDF("order_id", "member_id", "m_code","order_time", "price")
      .createOrReplaceTempView("orders")


    sparkSession.sql(
      """
        |
        |select member_id, count(order_time) as days
        |from orders
        |group by member_id
        |
      """.stripMargin)
      .show()


    val lives = Seq(
      ("o1", "u1", "m1",1599100484, 100.0),  //2020/9/3 10:34:44
      ("o2", "u1", "m2",1599014084, 120.0),  //2020/9/2 10:34:44
      ("o3", "u1", "m2",1598927684, 120.0),  //2020/9/1 10:34:44
      ("o4", "u1", "m2",1598931284, 120.0), //2020/9/1 11:34:44
      ("o5", "u1", "m2",1598924084, 120.0), //2020/9/1 9:34:44
      ("o6", "u2", "m2",1599100484, 100.0)

    )
    sparkSession.createDataFrame(lives).toDF("order_id", "member_id", "m_code","order_time", "price")
      .createOrReplaceTempView("lives")


    sparkSession.sql(
      """
        |
        |with d as (
        |   select member_id, from_unixtime(order_time, '%Y-%m-%d') as day
        |from lives
        |group by member_id,from_unixtime(order_time, '%Y-%m-%d')
        |)
        |
        |select member_id, count(day) as days
        |from d
        |group by member_id
        |
      """.stripMargin)
      .show()























    //      .select("member_id","order_id")
    //      .groupBy("member_id")
    //      .count()
    //      .filter(col("count").>(1))
    //      .show()

  }


}
