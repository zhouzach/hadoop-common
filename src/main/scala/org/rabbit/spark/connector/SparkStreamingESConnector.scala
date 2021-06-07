package org.rabbit.spark.connector

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark.rdd.Metadata._
import org.elasticsearch.spark.streaming._

import scala.collection.mutable

/**
 * Though, unlike RDDs, you are unable to read data out of Elasticsearch using a DStream due to the continuous nature of it.
 */
object SparkStreamingESConnector {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setAppName("es-connector").setMaster("local[*]")
    conf.set("es.index.auto.create", "true")
    conf.set("es.nodes", "localhost")
    conf.set("es.port", "9200")
    conf.set("es.query", """ {"query":{"match_all":{}}}""")

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(1))

    val numbers = Map("id"->1,"one" -> 1, "two" -> 2, "three" -> 3)
    val airports = Map("id"->2,"arrival" -> "Otopeni", "SFO" -> "San Fran")
//    val rdd = sc.makeRDD(Seq(numbers, airports))



    val json1 = """{"reason" : "business", "airport" : "SFO"}"""
    val json2 = """{"participants" : 5, "airport" : "OTP"}"""
//    val rdd = sc.makeRDD(Seq(json1, json2))


    val game = Map(
      "media_type" -> "game",
      "title" -> "FF VI",
      "year" -> "1994")
    val book = Map("media_type" -> "book","title" -> "Harry Potter","year" -> "2010")
    val cd = Map("media_type" -> "music","title" -> "Surfing With The Alien")
//    val rdd =sc.makeRDD(Seq(game, book, cd))

    val otpMeta = Map(ID -> 1)
    val mucMeta = Map(ID -> 2, VERSION -> "23")
    val sfoMeta = Map(ID -> 3)

    val otp = Map("iata" -> "OTP", "name" -> "Otopeni")
    val muc = Map("iata" -> "MUC", "name" -> "Munich")
    val sfo = Map("iata" -> "SFO", "name" -> "San Fran")
    val rdd = sc.makeRDD(Seq((otpMeta, otp), (mucMeta, muc), (sfoMeta, sfo)))

    val microbatches = mutable.Queue(rdd)

//    ssc.queueStream(microbatches).saveToEs("json-trips/_doc", Map("es.mapping.id" -> "id"))
//    ssc.queueStream(microbatches).saveJsonToEs("json-trips/_doc")
//    ssc.queueStream(microbatches).saveToEs("streaming-{media_type}/_doc")
    ssc.queueStream(microbatches).saveToEsWithMeta("airports1/_doc")

    ssc.start()
    ssc.awaitTermination()
  }

}
