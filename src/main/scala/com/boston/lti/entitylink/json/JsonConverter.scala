package com.boston.lti.entitylink.json

import java.nio.file.{Files, Paths}

import com.boston.lti.entitylink.ClueWeb09WarcUtils._

import scala.io.{BufferedSource, Source}

abstract class JsonConverter extends App {

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

  var iter = 0

  def bufferedReaderToJsonList(bufferedSource: BufferedSource): Seq[String]

  files.foreach { file =>
    val writer = java.nio.file.Files.newBufferedWriter(Paths.get(s"$outputDirectory/${file.getName}"))
    println(s"Processing file:$file")

    val bufferedReader: BufferedSource = Source.fromFile(file)
    val entityList = bufferedReaderToJsonList(bufferedReader)
    println(s"Total entity list size:${entityList.length}")

    println(s"Writing entity jsons to file")
    entityList.foreach { entityStr =>
      writer.write(entityStr)
      writer.write("\n")
      iter += 1
      if (iter % 1000 == 0) println(s"Written $iter records to file so far")
    }

    bufferedReader.close()
    writer.flush()
    writer.close()
  }


  println(s"A total of $iter entities jsons have been converted and written to file")
  println("Completed everything. Shutting down")

}
