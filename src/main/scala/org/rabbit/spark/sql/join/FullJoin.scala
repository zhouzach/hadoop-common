package org.rabbit.spark.sql.join

import org.rabbit.spark.sparkSession

object FullJoin {

  def main(args: Array[String]): Unit = {

    import sparkSession.implicits._
    Seq(
      "2020-10-01",
      "2020-10-02",
      "2020-10-03",
      "2020-10-04").toDF("dt")
      .createOrReplaceTempView("t_date")

    val volume = Seq(
      ("2020-10-01","ios", 100.0),
      ("2020-10-01","android", 120.0),
      ("2020-10-03","web", 80.0)
    )
    sparkSession.createDataFrame(volume).toDF("dt","channel", "volume")
      .createOrReplaceTempView("t_volume")

    val active = Seq(
//      ("ios", 10),
//      ("android", 12),
      ("web", 100)
    )
    sparkSession.createDataFrame(active).toDF("channel", "active_cnt")
      .createOrReplaceTempView("t_active")

//    sparkSession.sql(
//      """
//        |
//        |select *
//        |from t_volume v full join (select * from t_active where channel != 'web') a on  v.channel=a.channel
//        |""".stripMargin)
//      .show()
    sparkSession.sql(
      """
        |
        |select *
        |from t_date d full join (select * from t_volume where dt > '2020-10-04') v on  v.dt=d.dt
        |""".stripMargin)
      .show()

//    sparkSession.sql(
//      """
//        |
//        |select d.order_id, case when t.order_id is null then 0 else 1 end as label
//        |from orders d left join details t on  d.order_id=t.order_id
//        |""".stripMargin)
//      .show()


  }

}
