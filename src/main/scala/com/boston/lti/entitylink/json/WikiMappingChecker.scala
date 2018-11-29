package com.boston.lti.entitylink.json

import io.circe.{Error, Json}
import io.circe.parser.decode

import scala.io.{BufferedSource, Source}

object WikiMappingChecker extends App {
  val bufferedWikiMappingReader: BufferedSource = Source.fromFile("/home/ramithp/workspace/DirectedStudy/EntityLinkSearch/wiki_freebase_id_mapping")

//  val bufferedWarcReader: BufferedSource = Source.fromFile("/home/ramithp/workspace/DirectedStudy/EntityLinkSearch/00.warc.gz")
  val bufferedWarcReader: BufferedSource = Source.fromFile("/home/ramithp/workspace/DirectedStudy/EntityLinkSearch/YahooFELData/qry_tags/trec_annotations_fel.json")


  val wikiMappings = bufferedWikiMappingReader.getLines().toList.map(_.split("\t")(0)).toSet

  val entityIDSet: Set[String] = bufferedWarcReader.getLines().toSet.flatMap { jsonStr: String =>
    val decodedJson: Either[Error, Map[String, String]] = decode[Map[String, String]](jsonStr)
    decodedJson match {
      case Left(error) => println(s"Failed to parse str:$jsonStr")
        Set.empty[String]
      case Right(jsonMap) => jsonMap("tagme").split(" ").toSet
    }
  }

  println(s"EntityID set:${entityIDSet.size}")

  println(s"WikiMappings: ${wikiMappings.size}")

  println(s"Set diff size (entities - wikiMappings): ${entityIDSet.diff(wikiMappings).size}")
}