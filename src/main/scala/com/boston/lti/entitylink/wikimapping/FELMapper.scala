package com.boston.lti.entitylink.wikimapping

import io.circe.Decoder.Result
import io.circe.parser.decode
import io.circe.syntax._
import io.circe.{Error, Json}

import scala.io.BufferedSource


object FELMapper extends WikiMapper {


  override def bufferedReaderToJsonList(bufferedSource: BufferedSource): Seq[String] = {
    bufferedSource.getLines().toSeq.map { jsonStr =>
      val decodedJson: Either[Error, Map[String, Json]] = decode[Map[String, Json]](jsonStr)
      //Parse the json and generate the entityID list
      val (docID, entityIDList) = decodedJson match {
        case Left(error) => println(s"Failed to parse str:$jsonStr")
          ("", List.empty[String])
        case Right(jsonMap) =>
          val titleEntityList: List[String] = {
            val titleAnnotations: Result[List[Map[String, String]]] = jsonMap("titleAnnotations").as[List[Map[String, String]]]
            titleAnnotations match {
              case Left(error) => List.empty[String]
              case Right(entityMapList) => entityMapList.map(entityMap => entityMap("text"))
            }
          }

          val bodyEntityList: List[String] = {
            val bodyAnnotations: Result[List[Map[String, String]]] = jsonMap("bodyAnnotations").as[List[Map[String, String]]]
            bodyAnnotations match {
              case Left(error) => List.empty[String]
              case Right(entityMapList) => entityMapList.map(entityMap => entityMap("text"))
            }
          }
          val docID = jsonMap("docID").as[String].right.getOrElse("")

          val wikiMappingList: List[String] = (bodyEntityList ++ titleEntityList).flatMap { entityTitle =>
            wikiMappings.getOrElse(processTitle(entityTitle), List.empty[FreebaseMapping]).map(_.wikiId)
          }

          //Merge
          (docID, wikiMappingList)
      }
      Map("docno" -> Json.fromString(docID), "tagme" -> Json.fromString(entityIDList.mkString(" "))).asJson.noSpaces
    }
  }
}

