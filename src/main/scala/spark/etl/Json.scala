package spark.etl

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType

//http://bbs.bugcode.cn/t/74456
object Json {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()

  import sparkSession.implicits._

  def main(args: Array[String]): Unit = {


//    Seq("""{"A": "ae"}""", """{"A": ["ab", "cd"]}""")
//      .toDS.show()
//
//    StructType.fromDDL("A array<string>").foreach(println(_))
//    val field = StructType.fromDDL("A array<string>")("A")
//    println(s"field: $field")
//
    Seq("""{"A": "ae"}""", """{"A": ["ab", "cd"]}""")
      .toDS
      .select(

        from_json($"value", StructType.fromDDL("A array<string>"))("A"),
        array(get_json_object($"value", "$.A")),
        coalesce(
          // Attempt to parse value as array<string>
          from_json($"value", StructType.fromDDL("A array<string>"))("A"),
          // If the first one fails, try to extract it as string and enclose with array
          array(get_json_object($"value", "$.A"))).alias("A")
      ).show
  }

}
