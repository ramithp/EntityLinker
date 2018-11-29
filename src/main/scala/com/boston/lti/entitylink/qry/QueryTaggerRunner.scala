package com.boston.lti.entitylink.qry

import java.nio.file.Paths

import com.boston.lti.entitylink.EntityTagger
import com.boston.lti.entitylink.warc.FELRunner.args
import com.boston.lti.entitylink.warc.{EntityTaggerRunner, FELTagger, SemanticLRTagger, TagMeTagger}
import com.boston.lti.entitylink.warc.SemanticLRRunner.args
import io.circe.Decoder.Result
import io.circe.{Error, Json}
import io.circe.parser.decode
import io.circe.syntax._

import scala.io.Source

abstract class QueryTaggerRunner extends App {
  this: EntityTagger =>

  val queries = Source.fromFile(args(0)).getLines().toList
  val outputFile = args(1)

  val txtWriter = java.nio.file.Files.newBufferedWriter(Paths.get(s"$outputFile.txt"))
  val jsonWriter = java.nio.file.Files.newBufferedWriter(Paths.get(s"$outputFile.json"))


  var totalEntities = 0L
  queries.foreach { qLine =>
    val Array(qid, query) = qLine.split(":", 2)

    //Pre-process in some way?
    val (queryAnnotationsCount, queryAnnotations) = getAnnotations(query)

    //TODO Avoid this fight against my own classes
    val decodedJson = queryAnnotations.as[List[Map[String, String]]]
    //Parse the json and generate the entityID list
    val entityIDList = decodedJson match {
      case Left(_) => List.empty[Long]
      case Right(entityMapList) => entityMapList.map(entityMap => entityMap("id").toLong)
    }

    jsonWriter.write(Map("queryID" -> Json.fromString(qid), "annotations" -> queryAnnotations).asJson.noSpaces)
    jsonWriter.write("\n")

    txtWriter.write(s"$qid:::$query::: ${entityIDList.mkString(" ")}")
    txtWriter.write("\n")

    println(s"Found $queryAnnotationsCount entities for $qLine")
    totalEntities += queryAnnotationsCount
  }

  txtWriter.flush()
  jsonWriter.close()
}


object FELFileRunner extends QueryTaggerRunner with FELTagger {
  def modelFile = args(2)
}

object SemanticLRFileRunner extends QueryTaggerRunner with SemanticLRTagger {
  def modelFile = args(2)

  def vectorsFile = args(3)

  def entitiesFile = args(4)
}


object TagMeFileRunner extends QueryTaggerRunner with TagMeTagger