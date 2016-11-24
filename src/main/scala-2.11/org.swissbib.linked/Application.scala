package org.swissbib.linked

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.Metadata._

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
  val config = parser.parse(args, Config())
  val esConstants = (id: AnyRef) => Map("@type" -> "http://bibframe.org/vocab/Work",
    "@context" -> "http://data.swissbib.ch/work/context.jsonld",
    "@id" -> ("http://data.swissbib.ch/work/" + id))

  def mapMerger(a: Map[String, AnyRef], b: Map[String, AnyRef]): Map[String, AnyRef] = {
    b.keys.foldLeft(a)((agg, k) => (agg.getOrElse(k, None), b(k)) match {
      case (None, e2: AnyRef) => agg + (k -> e2)
      case (e1: String, e2: String) => agg + (k -> Traversable(e1, e2))
      case (e1: String, e2: Traversable[String]) => agg + (k -> (Traversable(e1) ++: e2))
      case (e1: Traversable[String], e2: String) => agg + (k -> (Traversable(e2) ++: e1))
      case (e1: Traversable[String], e2: Traversable[String]) => agg + (k -> (e1 ++: e2))
      case _ => throw new Error("Not supported!")
    })
  }

  case class Config(sparkMaster: String = "local[*]",
                    sparkHome: String = "",
                    esHost: String = "localhost:9200",
                    esCluster: String = "elasticsearch",
                    esIndex: String = "lsb",
                    esOriginType: String = "bibliographicResource",
                    esTargetType: String = "work",
                    esWorkFields: List[String] = "dct:contributor" :: "dct:title" :: Nil) {
    def getEsOriginType: String = esIndex + "/" + esOriginType

    def getEsTargetType: String = esIndex + "/" + esTargetType

    def createWorkFieldsMap(resId: String, docBody: scala.collection.Map[String, AnyRef]) =
      Map("bf:hasInstance" -> ("http://data.swissbib.ch/bibliographicResource/" + resId)) ++
        docBody.filterKeys(x => esWorkFields.contains(x))
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
        .map(x => Tuple2(x._2.get("work"), conf.createWorkFieldsMap(x._1, x._2)))
        // Third step: Group tuples with same work id
        .reduceByKey((acc, x) => mapMerger(acc, x))
        // Forth step: Merge values with same work id to a new Tuple2 and add some static fields
        .map(e => Tuple2(Map(ID -> e._1), e._2 ++ esConstants(e._1.get)))
        // Fifth step: Save the rearranged and merged tuples to Elasticsearch as documents of type work
        .saveToEsWithMeta(conf.getEsTargetType)
    //.saveAsTextFile("/swissbib_index/text")

    case None => throw new Error("No configuration settings!")
  }

}
