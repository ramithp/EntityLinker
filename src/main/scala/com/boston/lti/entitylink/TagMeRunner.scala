package com.boston.lti.entitylink

import java.io.BufferedWriter
import java.nio.file
import java.nio.file.{Files, Path, Paths}
import java.util
import java.util.{List, StringJoiner}

import it.acubelab.tagme.config.TagmeConfig
import it.acubelab.tagme._
import org.jsoup.Jsoup
import org.jwat.common.ContentType
import WarcUtils._

import scala.util._
import io.circe.syntax._
import it.acubelab.tagme.preprocessing.TopicSearcher

import scala.collection.mutable.ListBuffer

object TagMeRunner extends App {

  def tagMeAnnotateText(doc: String, parser: TagmeParser, disamb: Disambiguator, segmentation: Segmentation, rel: RelatednessMeasure) = {
    var entityCounter = 0
    val annotatedText = new AnnotatedText(doc)

    parser.parse(annotatedText)
    segmentation.segment(annotatedText)
    disamb.disambiguate(annotatedText, rel)
    // rho.calc(ann_text, rel);

    val annots = annotatedText.getAnnotations
    //val topicSearcher = new TopicSearcher(lang)

    val annotationsBuffer = new ListBuffer[String]
    import scala.collection.JavaConversions._
    for (a <- annots) {
      if (a.isDisambiguated) {
        entityCounter += 1
        annotationsBuffer.add(String.valueOf(a.getTopic))
      }
    }
    (entityCounter, annotationsBuffer)
  }

  if (args.length != 2) throw new IllegalArgumentException("Usage: [dataDirectory] [outputDirectory]")

  val dataDirectory = args(0)
  val outputDirectory = args(1)

  val startTime = System.currentTimeMillis()

  val dirPath = {
    val path = Paths.get(outputDirectory)
    if (Files.notExists(path))
      Files.createDirectory(Paths.get(outputDirectory))
    path
  }

  val files = listFiles(dataDirectory)

  val lang = "en"
  TagmeConfig.init()
  val parser = new TagmeParser(lang, true)
  val disamb = new Disambiguator(lang)
  val segmentation = new Segmentation
  val rho = new RhoMeasure
  val rel = it.acubelab.tagme.RelatednessMeasure.create(lang)
  val topicSearcher = new TopicSearcher(lang)


  var docCount = 0
  var docCount200 = 0
  var titleEntities = 0L
  var bodyEntities = 0L
  var failureCount = 0L

  files.foreach { file =>
    val writer = java.nio.file.Files.newBufferedWriter(Paths.get(s"$outputDirectory/${file.getName}"))
    println(s"Processing file:$file")
    processFile(file.getAbsolutePath) { (warcRecord, headers, payloadStream) =>
      docCount += 1
      if (headers != null && headers.statusCode == 200) {
        docCount200 += 1
        //println(headers)
        val charset = Try(ContentType.parseContentType(headers.contentType).getParameter("charset"))
          .getOrElse("utf-8")
        Try(scala.io.Source.fromInputStream(payloadStream, charset).mkString) match {
          case Success(html) =>
            val doc = Option(warcRecord.header.warcRecordIdUri) match {
              case Some(requestUri) => Jsoup.parse(html, requestUri.toString)
              case None => Jsoup.parse(html)
            }

            val trecID = Option(warcRecord.getHeader("WARC-TREC-ID").value).getOrElse(docCount.toString)

            val title = Option(doc.title()).getOrElse("")
            val body = Option(doc.body().text()).getOrElse("")
            val (titleEntitiesCount, titleAnnotations) = tagMeAnnotateText(title, parser, disamb, segmentation, rel)
            val (bodyEntitiesCount, bodyAnnotations) = tagMeAnnotateText(body, parser, disamb, segmentation, rel)

            writer.write(Map("docID" -> trecID, "tagMeT" -> s"[${titleAnnotations.mkString(",")}]",
              "tagMeB" -> s"[${bodyAnnotations.mkString(",")}]").asJson.noSpaces)
            writer.write("\n")
            docCount += 1
            titleEntities += titleEntitiesCount
            bodyEntities += bodyEntitiesCount
            if (docCount % 1000 == 0) println(s"Completed $docCount documents of which $docCount200 were status 200 in ${(System.currentTimeMillis() - startTime) / 1000} seconds. Failed on $failureCount along the way :(")
            if (docCount % 100000 == 0) println(s"Sanity test: title:$title, body:$body")
          case Failure(ex) => failureCount += 1
        }
      }
    }
    writer.flush()
    writer.close()
  }

  println("Completed everything. Shutting down")
}