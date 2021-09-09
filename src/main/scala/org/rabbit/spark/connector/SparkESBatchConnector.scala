package org.rabbit.spark.connector

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._

//https://www.elastic.co/guide/en/elasticsearch/hadoop/current/spark.html
object SparkESBatchConnector {

  def main(args: Array[String]): Unit = {


    val conf = new SparkConf().setAppName("es-connector").setMaster("local[*]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "localhost")
    conf.set("es.port", "9200")
    conf.set("es.query", """ {"query":{"match_all":{}}}""")
    val sc = new SparkContext(conf)

    read(sc, "apm-7.13.1-metric-000001")
      .collect().foreach(println(_))


  }

  def read(sc: SparkContext, index: String)={
//    sc.esRDD("airports/_doc","?q=s*")

    sc.esJsonRDD(s"$index/_doc")
  }

  def write(sc: SparkContext) = {

    // one map to one document
    val numbers = Map("eid" -> 32, "two1" -> 21, "three1" -> 31)
    val airports = Map("eid" -> 36, "SFO1" -> "San Fran1")


    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""

    //    sc.makeRDD(Seq(json1, json2))
    //      .saveJsonToEs("spark/_doc")

    //    sc.makeRDD(
    //      Seq(numbers, airports)
    //    ).saveToEs("spark/_doc", Map("es.mapping.id" -> "eid"))


    val game = Map(
      "media_type"->"game",
      "title" -> "FF VI",
      "year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")

    //    sc.makeRDD(Seq(game, book, cd)).saveToEs("my-collection-{media_type}/_doc")

    import org.elasticsearch.spark.rdd.Metadata._
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")

    // metadata for each document
    // note it's not required for them to have the same structure
    val mucMeta = Map(ID -> 1, VERSION -> "23")
    val sfoMeta = Map(ID -> 2)

    val airportsRDD = sc.makeRDD(
      Seq( (mucMeta, muc), (sfoMeta, sfo)))
    airportsRDD.saveToEsWithMeta("airports/_doc")
  }

}
