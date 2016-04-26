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
    val esTargetType = "work2"
    def esIndexType(t: String)(implicit i: String) = i + "/" + t

    val sparkConfig = new SparkConf()
      .setMaster(master)
      .setAppName(appName)
      .setSparkHome(sparkHome)
      .set("es.nodes", esNodes)
      .set("es.port", esPort)
      .set("es.mapping.date.rich", "false")

    val valueCollector = (agg: mutable.Map[String, mutable.Buffer[String]], elem: Map[String, AnyRef]) => {
      for (key <- elem.keys) {
        elem(key) match {
          case s: String => if (s != "") {
            agg(key) ++= mutable.Buffer(s)
            agg(key) = agg(key).distinct
          }
          case b: mutable.Buffer[String] =>
            agg(key) ++= b.filter(_ != "")
            agg(key) = agg(key).distinct
          case _ => throw new Exception("Not supported!")
        }
      }
      agg
    }

    val array2String: Tuple2[String, AnyRef] = (key: String, value: mutable.Buffer[String]) => {
      Tuple2(key, if (value.length == 1) value.head else value)
    }

    new SparkContext(sparkConfig)
      .esRDD(esIndexType(esOriginType), "?q=_exists_:work")
      .map(x => Tuple2(x._2.get("work"),
        Map("bf:hasInstance" -> ("http://data.swissbib.ch/bibliographicResource/" + x._1),
          "dct:contributor" -> x._2.getOrElse("dct:contributor", ""),
          "dct:title" -> x._2.getOrElse("dct:title", "")
        )
      ))
      .groupByKey()
      .map(e =>
        Tuple2(Map(ID -> e._1), e._2.foldLeft(mutable.Map("bf:hasInstance" -> mutable.Buffer[String](), "dct:contributor" -> mutable.Buffer[String](), "dct:title" -> mutable.Buffer[String]()))((x, y) => valueCollector(x, y)
            //.mapValues(v => if(v.length == 1) v.head else v)(scala.collection.breakOut)

        + ("@type" -> mutable.Buffer("http://bibframe.org/vocab/Work"),
          "@context" -> mutable.Buffer("http://data.swissbib.ch/work/context.jsonld"),
          "@id" -> mutable.Buffer("http://data.swissbib.ch/work/" + e._1))
        ))
      )
      .saveToEsWithMeta(esIndexType(esTargetType))
  }
}
