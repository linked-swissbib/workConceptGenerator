package org.swissbib.linked

import org.apache.spark.{SparkConf, SparkContext}
import org.elasticsearch.spark._
import org.elasticsearch.spark.rdd.Metadata._

/**
  * @author Sebastian Schüpbach
  * @version 1.1
  *
  *          Created on 25.03.16
  */
object Application extends App {

  val parser = new scopt.OptionParser[Config]("scopt") {
    head("WorkContextGenerator", "1.0")
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

  def esConstants(id: AnyRef) = Map("@type" -> "http://bibframe.org/vocab/Work",
    "@context" -> "http://data.swissbib.ch/work/context.jsonld",
    "@id" -> ("http://data.swissbib.ch/work/" + id))

  def distinct[T](t: Traversable[T], a: Traversable[T] = Nil): Traversable[T] = {
    def checkUniqueness(agg: Traversable[T], h: T, t: Traversable[T]): Traversable[T] = (h, t) match {
      case (x, head :: tail) if x == head => checkUniqueness(agg, x, tail)
      case (x, head :: tail) => checkUniqueness(Traversable(head) ++: agg, x, tail)
      case (x, _) => agg
    }
    t match {
      case head :: tail => distinct(checkUniqueness(Nil, head, tail), Traversable(head) ++: a)
      case head => head ++: a
    }
  }

  def mapMerger[T, U >: AnyRef](a: Map[T, U], b: Map[T, U]): Map[T, U] = {
    b.keys.foldLeft(a)((agg, k) => (agg.getOrElse(k, None), b(k)) match {
      case (None, e2: U) => agg + (k -> e2)
      case (e1: T, e2: T) => agg + (k -> distinct(Traversable(e1, e2)))
      case (e1: T, e2: Traversable[T]) => agg + (k -> distinct(Traversable(e1) ++: e2))
      case (e1: Traversable[T], e2: T) => agg + (k -> distinct(Traversable(e2) ++: e1))
      case (e1: Traversable[T], e2: Traversable[T]) => agg + (k -> distinct(e1 ++: e2))
      case _ => throw new Error("Not supported!")
    })
  }

  case class Config(sparkHome: String = "",
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
      new SparkContext(new SparkConf().setAppName("Work Concept Generator"))
        // First step: Get all documents which contain a field work
        .esRDD(conf.getEsOriginType, "?q=_exists_:work")
        // Second step: Only take required values (i.e. the work id, the id and the title of the referring resource id and
        // ids of contributors
        .map(x => Tuple2(x._2.get("work"), conf.createWorkFieldsMap(x._1, x._2)))
        // Third step: Group tuples with same work id
        .reduceByKey((acc, x) => mapMerger(acc, x))
        // Forth step: Merge values with same work id to a new Tuple2 and add some static fields
        .map(e => (Map(ID -> e._1.get), e._2 ++ esConstants(e._1.get)))
        // Fifth step: Save the rearranged and merged tuples to Elasticsearch as documents of type work
        .saveToEsWithMeta(conf.getEsTargetType)
    //.saveAsTextFile("/swissbib_index/text")
    case None => throw new Error("No configuration settings!")
  }

}
