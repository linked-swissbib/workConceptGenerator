package org.swissbib.linked

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
object Application extends App {

  val parser = new scopt.OptionParser[Config]("scopt") {
    head("WorkContextGenerator", "1.0")
    opt[String]("sparkMaster")
      .optional()
      .action((x, c) => c.copy(sparkMaster = x))
      .text("Spark master URL")
    opt[String]("sparkHome")
      .optional()
      .action((x, c) => c.copy(sparkHome = x))
      .text("Path to Spark binary (if in local mode")
    opt[String]("esHost")
      .optional()
      .action((x, c) => c.copy(esHost = x))
      .validate(x =>
        if (x.contains(":")) success
        else failure("Option --esHost must be of kind host:port"))
      .text("Elasticsearch hostname:post")
    opt[String]("esCluster")
      .optional()
      .action((x, c) => c.copy(esCluster = x))
      .text("Elasticsearch clustername")
    opt[String]("esIndex")
      .optional()
      .action((x, c) => c.copy(esIndex = x))
      .text("Elasticsearch indexname")
    opt[String]("esOriginType")
      .optional()
      .action((x, c) => c.copy(esOriginType = x))
      .text("Name of type which contains the data needed for work creation")
    opt[String]("esTargetType")
      .optional()
      .action((x, c) => c.copy(esTargetType = x))
      .text("Name of type which will contain generated concept")
  }
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

  val mapCreator = (res: String, kvpair: scala.collection.Map[String, AnyRef]) => {
    var tempMap = Map[String, AnyRef]("bf:hasInstance" -> res)
    if (kvpair.contains("dct:contributor")) tempMap += ("dct:contributor" -> kvpair("dct:contributor"))
    if (kvpair.contains("dct:title")) tempMap += ("dct:title" -> kvpair("dct:title"))
    tempMap
  }

  val config = parser.parse(args, Config())

  case class Config(sparkMaster: String = "local[*]",
                    sparkHome: String = "",
                    esHost: String = "localhost:9200",
                    esCluster: String = "elasticsearch",
                    esIndex: String = "lsb",
                    esOriginType: String = "bibliographicResource",
                    esTargetType: String = "work") {
    def getEsOriginType: String = esIndex + "/" + esOriginType

    def getEsTargetType: String = esIndex + "/" + esTargetType
  }

  config match {
    case Some(conf) =>
      val sparkConfig = new SparkConf()
        .setMaster(conf.sparkMaster)
        .setAppName("Work Concept Generator")
        .set("es.nodes", conf.esHost.split(":")(0))
        .set("es.port", conf.esHost.split(":")(1))
        .set("es.mapping.date.rich", "false")
        .set("spark.executor.memory", "12g")
      if (conf.sparkHome != "") sparkConfig.setSparkHome(conf.sparkHome)

      new SparkContext(sparkConfig)
        // First step: Get all documents which contain a field work
        .esRDD(conf.getEsOriginType, "?q=_exists_:work")
        // Second step: Only take required values (i.e. the work id, the id and the title of the referring resource id and
        // ids of contributors
        .map(x => Tuple2(x._2.get("work"), mapCreator("http://data.swissbib.ch/bibliographicResource/" + x._1, x._2)))
        // Third step: Group tuples with same work id
        .groupByKey()
        // Forth step: Merge values with same work id to a new Tuple2 and add some static fields
        .map(e =>
        Tuple2(Map(ID -> e._1), e._2.reduce(valueCollector)
          +("@type" -> "http://bibframe.org/vocab/Work",
          "@context" -> "http://data.swissbib.ch/work/context.jsonld",
          "@id" -> ("http://data.swissbib.ch/work/" + e._1.get)))
      )
        // Fifth step: Save the rearranged and merged tuples to Elasticsearch as documents of type work
        .saveToEsWithMeta(conf.getEsTargetType)
    //.saveAsTextFile("/swissbib_index/text")

    case None =>
    // Errors
  }

}
