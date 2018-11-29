package com.boston.lti.entitylink.warc

import java.nio.file.{Files, Paths}

import com.boston.lti.entitylink.warc.ClueWeb09WarcUtils.{listFiles, processFile}
import io.circe.Json
import io.circe.syntax._
import com.boston.lti.entitylink.EntityTagger

abstract class EntityTaggerRunner extends App {
  this: EntityTagger =>

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


  val alreadyCompletedFiles = Set("00.warc.gz", "06.warc.gz", "13.warc.gz", "20.warc.gz", "26.warc.gz", "33.warc.gz", "41.warc.gz", "47.warc.gz", "55.warc.gz", "63.warc.gz", "69.warc.gz", "76.warc.gz", "84.warc.gz", "91.warc.gz", "97.warc.gz", "01.warc.gz", "07.warc.gz", "14.warc.gz", "21.warc.gz", "27.warc.gz", "34.warc.gz", "42.warc.gz", "48.warc.gz", "56.warc.gz", "64.warc.gz", "70.warc.gz", "78.warc.gz", "85.warc.gz", "92.warc.gz", "98.warc.gz", "02.warc.gz", "08.warc.gz", "15.warc.gz", "22.warc.gz", "28.warc.gz", "36.warc.gz", "43.warc.gz", "49.warc.gz", "57.warc.gz", "65.warc.gz", "71.warc.gz", "79.warc.gz", "86.warc.gz", "93.warc.gz", "99.warc.gz", "03.warc.gz", "09.warc.gz", "17.warc.gz", "23.warc.gz", "29.warc.gz", "37.warc.gz", "44.warc.gz", "51.warc.gz", "59.warc.gz", "66.warc.gz", "72.warc.gz", "80.warc.gz", "88.warc.gz", "94.warc.gz", "04.warc.gz", "10.warc.gz", "18.warc.gz", "24.warc.gz", "30.warc.gz", "38.warc.gz", "45.warc.gz", "52.warc.gz", "60.warc.gz", "67.warc.gz", "74.warc.gz", "81.warc.gz", "89.warc.gz", "95.warc.gz", "05.warc.gz", "11.warc.gz", "19.warc.gz", "25.warc.gz", "32.warc.gz", "40.warc.gz", "46.warc.gz", "53.warc.gz", "61.warc.gz", "68.warc.gz", "75.warc.gz", "82.warc.gz", "90.warc.gz", "96.warc.gz")

  files.foreach { file =>
    if (alreadyCompletedFiles.contains(file.getName)) println(s"Already completed ${file.getName}, skipping")
    else {
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
  }
  println(s"A total of $titleEntities title entities and $bodyEntities body entities were annotated")
  println("Completed everything. Shutting down")
}