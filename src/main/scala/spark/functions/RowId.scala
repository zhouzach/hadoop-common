package spark.functions

import spark.sparkSession
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType, StructField, LongType}
import org.apache.spark.sql.functions.monotonically_increasing_id

object RowId {

  import sparkSession.implicits._

  val df = Seq(("a", -1.0), ("b", -2.0), ("c", -3.0)).toDF("foo", "bar")

  def zipWithUniqueId(): Unit = {
    // 获取df 的表头
    val s = df.schema

    // 将原表转换成带有rdd,
    //再转换成带有id的rdd,
    //再展开成Seq方便转化成 Dataframe
    val rows = df.rdd.zipWithUniqueId.map { case (r: Row, id: Long) => Row.fromSeq(id +: r.toSeq) }

    // 再由 row 根据原表头进行转换
    val dfWithPK = sparkSession.createDataFrame(rows, StructType(StructField("id", LongType, false) +: s.fields))
    dfWithPK.show()
  }

  def increasingId() = {
    df.withColumn("id", monotonically_increasing_id).show()
  }

  def main(args: Array[String]): Unit = {

    zipWithUniqueId()

    increasingId()
  }

}
