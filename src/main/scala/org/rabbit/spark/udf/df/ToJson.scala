package org.rabbit.spark.udf.df

import com.google.gson._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ToJson {

  val sparkSession: SparkSession = SparkSession.builder.appName("Simple Application")
    .master("local")
    //    .enableHiveSupport()
    .getOrCreate()

  def main(args: Array[String]): Unit = {

    val toJsonArrayLambda = udf((obj: AnyRef) => {
      lazy val GSON: Gson = new GsonBuilder()
        //        .setPrettyPrinting()
        //      .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
        .disableHtmlEscaping()
        .addSerializationExclusionStrategy(new SpecificFieldExclusionStrategy())
        .create()
      GSON.toJson(obj)
    })

    val toJsonArrayUdf = udf(toJsonArray _)

    val userData = Array(("Leo", 16), ("Marry", 21), ("Jack", 14), ("Tom", 18))
    val userDF = sparkSession.createDataFrame(userData).toDF("name", "age")

    userDF.withColumn("names", typedLit(Seq("Leo", "Jack", "Tom")))
      .withColumn("ages", typedLit(Seq(1, 2, 3, 4, 5)))
      //      .withColumn("toJsonNamesLambda", toJsonArrayLambda(col("names")))
      //      .withColumn("toJsonAgesLambda", toJsonArrayLambda(col("ages")))
      .withColumn("toJsonAges", toJsonArrayUdf(col("ages")))
      .collect()
      .foreach(println(_))

    userDF.withColumn("names", typedLit(Seq("Leo", "Jack", "Tom")))
      .withColumn("ages", typedLit(Seq(1, 2, 3, 4, 5)))
      //      .withColumn("toJsonNamesLambda", toJsonArrayLambda(col("names")))
      //      .withColumn("toJsonAgesLambda", toJsonArrayLambda(col("ages")))
      .toJSON
      .collect()
      .foreach(println(_))

    userDF.select(col("*"), struct("name", "age").alias("people"))
      .toJSON
      .collect()
      .foreach(println(_))

    userDF.select(col("*"), array("name", "age").alias("people"))
      .toJSON
      .collect()
      .foreach(println(_))

    userDF.select(col("*"), map(col("name"), col("age")).alias("people"))
      .toJSON
      .collect()
      .foreach(println(_))


  }


  def toJsonArray(obj: AnyRef) = {
    lazy val GSON: Gson = new GsonBuilder()

      //      .setPrettyPrinting()
      //      .setFieldNamingPolicy(FieldNamingPolicy.UPPER_CAMEL_CASE)
      .disableHtmlEscaping()
      .addSerializationExclusionStrategy(new SpecificFieldExclusionStrategy())
      .create()

    GSON.toJson(obj)

  }

}




class SpecificFieldExclusionStrategy() extends ExclusionStrategy {

  import com.google.gson.FieldAttributes

  def shouldSkipClass(clazz: Class[_]): Boolean = false

  def shouldSkipField(f: FieldAttributes): Boolean = f.getName == "bitmap$0"
}
