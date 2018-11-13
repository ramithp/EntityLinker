package com.boston.lti.entitylink.json

import io.circe.Decoder.Result
import io.circe._
import io.circe.parser._
import io.circe.syntax._

import scala.io.BufferedSource

object TagMeNewConverter extends JsonConverter {
  override def bufferedReaderToJsonList(bufferedSource: BufferedSource) = bufferedSource.getLines().toSeq.map { jsonStr =>
    val decodedJson: Either[Error, Map[String, Json]] = decode[Map[String, Json]](jsonStr)
    //Parse the json and generate the entityID list
    val (docID, entityIDList) = decodedJson match {
      case Left(error) => println(s"Failed to parse str:$jsonStr")
        ("", List.empty[Long])
      case Right(jsonMap) =>
        val titleEntityList: List[Long] = {
          val titleAnnotations: Result[List[String]] = jsonMap("titleAnnotations").as[List[String]]
          titleAnnotations match {
            case Left(error) => List.empty[Long]
            case Right(entityMapList) => entityMapList.map(_.toLong)
          }
        }

        val bodyEntityList: List[Long] = {
          val bodyAnnotations: Result[List[String]] = jsonMap("bodyAnnotations").as[List[String]]
          bodyAnnotations match {
            case Left(error) => List.empty[Long]
            case Right(entityMapList) => entityMapList.map(_.toLong)
          }
        }
        val docID = jsonMap("docID").as[String].right.getOrElse("")
        //Merge
        (docID, bodyEntityList ++ titleEntityList)
    }
    Map("docno" -> Json.fromString(docID), "tagme" -> Json.fromString(entityIDList.mkString(" "))).asJson.noSpaces
  }
}
