package org.rabbit.spark.sql.join

import org.rabbit.spark.sparkSession

object FullJoinByTwoIds {

  def main(args: Array[String]): Unit = {

    import sparkSession.implicits._
    Seq(
      (4,"2020-10-01"),
      (5,"2020-10-02"),
      (6,"2020-10-03"),
      (7,"2020-10-04")
    ).toDF("uid","dt")
      .createOrReplaceTempView("t_user")

    val volume = Seq(
      (1,"ios", 100.0),
      (2,"android", 120.0),
      (3,"web", 80.0)
    )
    sparkSession.createDataFrame(volume).toDF("os_id","channel", "volume")
      .createOrReplaceTempView("t_volume")


    sparkSession.sql(
      """
        |
        |select *
        |from t_user u full join t_volume v on  u.uid!=v.os_id
        |""".stripMargin)
      .show()








  }

}
