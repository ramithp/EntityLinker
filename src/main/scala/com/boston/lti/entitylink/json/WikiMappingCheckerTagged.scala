package com.boston.lti.entitylink.json

import java.net.URLDecoder

import com.boston.lti.entitylink.wikimapping.{FreebaseMapping, WikiEntry}
import io.circe.parser.decode
import io.circe.{Error, Json}

import scala.io.{BufferedSource, Source}

object WikiMappingTaggedQueries extends App {
  val bufferedWikiMappingReader: BufferedSource = Source.fromFile("/home/ramithp/workspace/DirectedStudy/EntityLinkSearch/wiki_freebase_id_mapping")
  val bufferedWarcReader: BufferedSource = Source.fromFile("/home/ramithp/workspace/DirectedStudy/EntityLinkSearch/YahooFELData/qry_tags/trec_annotations_fel.txt")


  val wikiMappings = bufferedWikiMappingReader.getLines().toList.map(_.split("\t")(0)).toSet

  val entityIDSet: Set[String] = bufferedWarcReader.getLines().toSet.flatMap { line: String =>
    line.split(":::")(2).split(" ").map(_.trim).toSet
  }

  println(s"EntityID set:${entityIDSet.size}")

  println(s"WikiMappings: ${wikiMappings.size}")

  println(s"Set diff size (entities - wikiMappings): ${entityIDSet.diff(wikiMappings).size}")
}


object WikiMappingTaggedJsonQueries extends App {
  val bufferedWikiMappingReader: BufferedSource = Source.fromFile("/home/ramithp/workspace/DirectedStudy/EntityLinkSearch/wiki_freebase_id_mapping")
  val bufferedWarcReader: BufferedSource = Source.fromFile("/home/ramithp/workspace/DirectedStudy/EntityLinkSearch/YahooFELData/qry_tags/trec_annotations_tagme.json")


  val wikiMappings = bufferedWikiMappingReader.getLines()
    .toList
    .map(_.split("\t")(0))
    .toSet

  val entityIDSet: Set[String] = bufferedWarcReader.getLines().toSet.flatMap { jsonStr: String =>
    val decodedJson: Either[Error, Map[String, Json]] = decode[Map[String, Json]](jsonStr)

    decodedJson match {
      case Left(error) => println(s"Failed to parse str:$jsonStr")
        Set.empty[String]
      case Right(jsonMap) =>


        val inter: Either[Error, List[String]] = decode[List[String]](jsonMap("annotations").noSpaces)
        inter match {
          case Left(err) =>
            Set.empty[String]
          case Right(list) => list.toSet
        }
    }
  }

  println(s"EntityID set:${entityIDSet.size}")

  println(s"WikiMappings: ${wikiMappings.size}")

  println(s"Set diff size (entities - wikiMappings): ${entityIDSet.diff(wikiMappings).size}")
}


object JsonQueries extends App {
  val bufferedWikiMappingReader: BufferedSource = Source.fromFile("/home/ramithp/workspace/DirectedStudy/EntityLinkSearch/wiki_freebase_id_mapping")
  val bufferedWarcReader: BufferedSource = Source.fromFile("/home/ramithp/workspace/DirectedStudy/EntityLinkSearch/YahooFELData/qry_tags/trec_annotations_fel.json", "UTF-8")

  val wikiMappings = bufferedWikiMappingReader.getLines()
    .toList
    .map(_.split("\\s+", 3))
    .map { case Array(x, y, z) =>
      FreebaseMapping(x, y, z
        .replace("_", " ")
        .replace("(", "")
        .replace(")", "")
        .replace(" ", "")
        .replace(",", "")
        .toLowerCase.trim)
    }.groupBy(_.text)

  println(wikiMappings.mapValues(_.length).values.sum)

  val entityIDSet: Map[String, List[WikiEntry]] = bufferedWarcReader.getLines().flatMap { jsonStr: String =>
    val decodedJson: Either[Error, Map[String, Json]] = decode[Map[String, Json]](jsonStr)

    decodedJson match {
      case Left(error) => println(s"Failed to parse str:$jsonStr")
        Map.empty[String, List[WikiEntry]]
      case Right(jsonMap) =>
        val inter: Either[Error, List[Map[String, String]]] = decode[List[Map[String, String]]](jsonMap("annotations").noSpaces)
        inter match {
          case Left(err) => Map.empty[String, List[WikiEntry]]
          case Right(encodedListMap) =>
            encodedListMap
              .map(x => WikiEntry(x("id"),
                URLDecoder.decode(x("text"), "utf-8")
                  //process the decoded word
                  .replace("_", " ")
                  .replace("(", "")
                  .replace(")", "")
                  .replace(",", "")
                  .replace(" ", "")
                  .toLowerCase
                  .trim))
              .groupBy(_.text)
        }
    }
  }.toMap

  println(s"EntityID set:${entityIDSet.size}")

  println(s"WikiMappings: ${wikiMappings.size}")

  wikiMappings.filterKeys(_.startsWith("antenna")).foreach(println)

  println()
  println()

  val set = entityIDSet.filterKeys { freebaseMapping => !wikiMappings.contains(freebaseMapping) }
  val set2 = wikiMappings.filterKeys { freebaseMapping => set.contains(freebaseMapping) }

  println(s"Set diff size (entities - wikiMappings): ${set.size}")
  println(s"Set diff size (entities - wikiMappings): ${set}")
  println(s"Set diff size (entities - wikiMappings): ${set2}")

}