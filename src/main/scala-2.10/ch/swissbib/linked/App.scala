package ch.swissbib.linked

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.Metadata._

import scala.collection.mutable

/**
  * @author Sebastian Schüpbach
  * @version 0.1
  *
  *          Created on 25.03.16
  */
object App {

  def main(args: Array[String]): Unit = {
    val master = "local[7]"
    val appName = "Work Concept Generator"
    val sparkHome = "/usr/local/spark"
    val esNodes = "localhost"
    val esPort = "9200"
    implicit val esIndex = "testsb_160421"
    val esOriginType = "bibliographicResource"
    val esTargetType = "work9"
    def esIndexType(t: String)(implicit i: String) = i + "/" + t

    val sparkConfig = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .setSparkHome(sparkHome)
      .set("es.nodes", esNodes)
      .set("es.port", esPort)
      .set("es.mapping.date.rich", "false")
    val sparkContext = new SparkContext(sparkConfig)

    val valueCollector = (agg: Map[String, AnyRef], elem: Map[String, AnyRef]) => {
      //val rMap: mutable.Map[String, AnyRef] = mutable.Map("bf:hasInstance" -> "", "dct:contributor" -> "", "dct:title" -> "")
      val rMap: mutable.Map[String, AnyRef] = mutable.Map()
      for (key <- elem.keys) {
        (agg.getOrElse(key, None), elem(key)) match {
          // First element to add is a string
          case (None, s: String) =>
            rMap(key) = s
          // Second element to add is a string, first one was a string
          case (k: String, s: String) =>
            rMap(key) = mutable.Buffer(k, s).distinct
          // First element to add is an array
          case (None, s: mutable.Buffer[String]) =>
            rMap(key) = s.distinct
          // Second element to add is an array, first one was a string
          case (k: String, s: mutable.Buffer[String]) =>
            rMap(key) = (s ++ mutable.Buffer(k)).distinct
          // Append a string element
          case (k: mutable.Buffer[String], s: String) =>
            rMap(key) = (mutable.Buffer(s) ++ k).distinct
          // Append an array element
          case (k: mutable.Buffer[String], s: mutable.Buffer[String]) =>
            rMap(key) = (k ++ s).distinct
          case (k, s) => throw new Exception("Not supported!")
        }
      }
      rMap.toMap
    }

    sparkContext
      // First step: Get all documents which contain a field work
      .esRDD(esIndexType(esOriginType), "?q=_exists_:work")
      // Second step: Only take required values (i.e. the work id, the id and the title of the referring resource id and
      // ids of contributors
      .map(x => Tuple2(x._2.get("work"),
      Map("bf:hasInstance" -> ("http://data.swissbib.ch/bibliographicResource/" + x._1),
        "dct:contributor" -> x._2.getOrElse("dct:contributor", ""),
        "dct:title" -> x._2.getOrElse("dct:title", "")
      )
    ))
      // Third step: Group tuples with same work id
      .groupByKey()
      // Forth step: Merge values with same work id to a new Tuple2 and add some static fields
      .map(e =>
      Tuple2(Map(ID -> e._1), e._2.reduce(valueCollector)
        +("@type" -> "http://bibframe.org/vocab/Work",
        "@context" -> "http://data.swissbib.ch/work/context.jsonld",
        "@id" -> ("http://data.swissbib.ch/work/" + e._1.get)))
    )
      // Sixth step: Save the rearranged and merged tuples to Elasticsearch as documents of type work
      .saveToEsWithMeta(esIndexType(esTargetType))
  }
}
