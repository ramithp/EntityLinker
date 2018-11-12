package com.boston.lti.entitylink.json

import io.circe.Decoder.Result
import io.circe._
import io.circe.parser._
import io.circe.syntax._
import scala.io.BufferedSource

object TagMeOldConverter extends JsonConverter {
  override def bufferedReaderToJsonList(bufferedSource: BufferedSource) = bufferedSource.getLines().toSeq.map { jsonStr =>
    val decodedJson: Either[Error, Map[String, String]] = decode[Map[String, String]](jsonStr)
    //Parse the json and generate the entityID list
    val (docID, entityIDList: Array[Long]) = decodedJson match {
      case Left(error) => println(s"Failed to parse str:$jsonStr")
        ("", Array.empty[Long])

      case Right(jsonMap) =>
        val titleEntityList: Array[Long] = {
          val entityString = jsonMap("tagMeT")
          if (entityString.length > 2) entityString.substring(1, entityString.length - 1).split(",").map(_.toLong)
          else Array.empty[Long]
        }

        val bodyEntityList: Array[Long] = {
          val entityString = jsonMap("tagMeB")
          if (entityString.length > 2) entityString.substring(1, entityString.length - 1).split(",").map(_.toLong)
          else Array.empty[Long]
        }

        val docID = jsonMap("docID")
        //Merge
        (docID, bodyEntityList ++ titleEntityList)
    }
    Map("docno" -> Json.fromString(docID), "tagme" -> entityIDList.asJson).asJson.noSpaces
  }
}
