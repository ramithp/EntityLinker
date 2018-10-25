package com.boston.lti.entitylink

import java.nio.file.{Files, Paths}

import com.boston.lti.entitylink.ClueWeb09WarcUtils.{listFiles, processFile}
import io.circe.Json
import io.circe.syntax._


abstract class EntityTaggerRunner extends App {

  def getAnnotations(text: String): (Long, Json)
  //def getAnnotationsJson(title:String, body: String): (Long, Long, String)


  if (args.length < 2) throw new IllegalArgumentException("Usage: [dataDirectory] [outputDirectory]")

  val dataDirectory = args(0)
  val outputDirectory = args(1)

  val files = listFiles(dataDirectory)
  val startTime = System.currentTimeMillis()


  val dirPath = {
    val path = Paths.get(outputDirectory)
    if (Files.notExists(path))
      Files.createDirectory(Paths.get(outputDirectory))
    path
  }

  var titleEntities = 0L
  var bodyEntities = 0L

  files.foreach { file =>
    val writer = java.nio.file.Files.newBufferedWriter(Paths.get(s"$outputDirectory/${file.getName}"))
    println(s"Processing file:$file")

    processFile(file.getAbsolutePath, startTime) { (trecID, body, title) =>
      val (titleEntitiesCount, titleAnnotations) = getAnnotations(title)
      val (bodyEntitiesCount, bodyAnnotations) = getAnnotations(body)

      writer.write(Map("docID" -> Json.fromString(trecID), "titleAnnotations" -> titleAnnotations,
        "bodyAnnotations" -> bodyAnnotations).asJson.noSpaces)
      writer.write("\n")
      titleEntities += titleEntitiesCount
      bodyEntities += bodyEntitiesCount
    }
    writer.flush()
    writer.close()
  }

  println(s"A total of $titleEntities title entities and $bodyEntities body entities were annotated")
  println("Completed everything. Shutting down")
}
