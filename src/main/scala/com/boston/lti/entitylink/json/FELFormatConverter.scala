package com.boston.lti.entitylink.json


import io.circe.Decoder.Result
import io.circe._
import io.circe.parser._
import io.circe.syntax._
import scala.io.BufferedSource

object FELFormatConverter extends JsonConverter {
  override def bufferedReaderToJsonList(bufferedSource: BufferedSource): Seq[String] = {
    bufferedSource.getLines().toSeq.map { jsonStr =>
      val decodedJson: Either[Error, Map[String, Json]] = decode[Map[String, Json]](jsonStr)
      //Parse the json and generate the entityID list
      val (docID, entityIDList) = decodedJson match {
        case Left(error) => println(s"Failed to parse str:$jsonStr")
          ("", List.empty[Long])
        case Right(jsonMap) =>
          val titleEntityList: List[Long] = {
            val titleAnnotations: Result[List[Map[String, String]]] = jsonMap("titleAnnotations").as[List[Map[String, String]]]
            titleAnnotations match {
              case Left(error) => List.empty[Long]
              case Right(entityMapList) => entityMapList.map(entityMap => entityMap("id").toLong)
            }
          }

          val bodyEntityList: List[Long] = {
            val bodyAnnotations: Result[List[Map[String, String]]] = jsonMap("bodyAnnotations").as[List[Map[String, String]]]
            bodyAnnotations match {
              case Left(error) => List.empty[Long]
              case Right(entityMapList) => entityMapList.map(entityMap => entityMap("id").toLong)
            }
          }
          val docID = jsonMap("docID").as[String].right.getOrElse("")
          //Merge
          (docID, bodyEntityList ++ titleEntityList)
      }
      Map("docno" -> Json.fromString(docID), "tagme" -> entityIDList.asJson).asJson.noSpaces
    }
  }
}
