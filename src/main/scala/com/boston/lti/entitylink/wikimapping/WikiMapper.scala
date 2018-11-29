package com.boston.lti.entitylink.wikimapping

import java.nio.file.{Files, Paths}

import com.boston.lti.entitylink.warc.ClueWeb09WarcUtils.listFiles

import scala.io.{BufferedSource, Source}


case class FreebaseMapping(wikiId: String, freeBaseID: String, text: String)

case class WikiEntry(wikiId: String, text: String)


abstract class WikiMapper extends App {

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

  val freeBaseWikiMappingFile = args(2)
  val bufferedWikiMappingReader: BufferedSource = Source.fromFile(freeBaseWikiMappingFile, "UTF-8")

  val wikiMappings: Map[String, List[FreebaseMapping]] = bufferedWikiMappingReader.getLines()
    .toList
    .map(_.split("\\s+", 3))
    .map { case Array(x, y, z) => FreebaseMapping(x, y, processTitle(z)) }
    .groupBy(_.text)

  println(s"Initialized wikipedia-freebase mapping of size:${wikiMappings.size}")


  var iter = 0

  def processTitle(unprocessedTitleStr: String) = unprocessedTitleStr
    .replace("_", " ")
    .replace("(", "")
    .replace(")", "")
    .replace(",", "")
    .replace(" ", "")
    .toLowerCase.trim

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
  println(s"Completed everything. Shutting down. Took ${(System.currentTimeMillis() - startTime) / 1000} seconds")

}

