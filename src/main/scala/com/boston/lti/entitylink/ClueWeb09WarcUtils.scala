package com.boston.lti.entitylink

import java.io._
import java.nio.charset.CodingErrorAction
import java.nio.file.{Files, Path, Paths}

import com.boston.lti.entitylink.TagMeRunner._
import org.jsoup.Jsoup
import org.jwat.common._
import org.jwat.gzip.{GzipEntry, GzipReader}
import org.jwat.warc.{WarcReader, WarcReaderFactory, WarcReaderUncompressed, WarcRecord}

import scala.io.Codec
import scala.util.{Failure, Success, Try}


object ClueWeb09WarcUtils {

  //Not sure what these mean, but came from ExtractFile.java from
  lazy val warcReader: WarcReaderUncompressed = {
    val warcReader = WarcReaderFactory.getReaderUncompressed
    warcReader.setWarcTargetUriProfile(UriProfile.RFC3986_ABS_16BIT_LAX)
    warcReader.setBlockDigestEnabled(true)
    warcReader.setPayloadDigestEnabled(true)
    warcReader.setRecordHeaderMaxSize(8192)
    warcReader.setPayloadHeaderMaxSize(32768)
    warcReader
  }

  @throws[IOException]
  def listFiles(directoryName: String, files: scala.collection.mutable.ListBuffer[File] = scala.collection.mutable.ListBuffer.empty[File]): List[File] = {
    val directory = new File(directoryName)
    // get all the files from a directory
    println(directory)
    val fList = directory.listFiles
    println(fList.toList)

    for (file <- fList) {
      if (file.isFile) files.append(file)
      else if (file.isDirectory) {
        listFiles(file.getAbsolutePath, files)
      }
    }
    files.toList
  }


  def foundEncoding(inputStream: InputStream, contentType: String) = {
    val charset = Option(Try(ContentType.parseContentType(contentType).getParameter("charset"))
      .toOption.getOrElse("UTF-8")).getOrElse("UTF-8")
    Try(scala.io.Source.fromInputStream(inputStream, charset).mkString)
  }

  def utf8(inputStream: InputStream) = {
    implicit val codec = Codec("UTF-8")
    codec.onMalformedInput(CodingErrorAction.IGNORE)
    Try(scala.io.Source.fromInputStream(inputStream).mkString)
  }

  //Counters for documenting progress
  var docCount = 0
  var docCount200 = 0
  var failureCount = 0L
  var gzipEntries = 0

  //Largely derived from org.jwat.tools.tasks.extract
  //Takes a function that acts on a (trecID, title, body) Tuple3
  //If title or body cannot be extracted, "" is returned
  def processFile(filePath: String, startTime: Long)(action: (String, String, String) => Unit) = {
    //Read the file and get an inputstream
    val raf = new RandomAccessFile(filePath, "r")
    val rafin = new RandomAccessFileInputStream(raf)
    val pbin = new ByteCountingPushBackInputStream(new BufferedInputStream(rafin, 8192), 32)

    //Since our files are almost always *.warc.gz
    if (GzipReader.isGzipped(pbin)) {
      println("Expected format of file gz found")

      val gzipReader = new GzipReader(pbin)
      var in: InputStream = null
      var gzipEntry = gzipReader.getNextEntry

      //Check if the entry we received is valid
      while (gzipEntry != null) {
        in = new ByteCountingPushBackInputStream(new BufferedInputStream(gzipEntry.getInputStream, 8192), 32)
        var warcRecord = warcReader.getNextRecordFrom(in, gzipEntry.getStartOffset)

        //Check if the warc record we parsed is valid
        while (warcRecord != null) {
          val payload = warcRecord.getPayload
          var payloadStream: InputStream = null
          if (payload != null) {
            val httpHeader = warcRecord.getHttpHeader
            if (httpHeader != null) payloadStream = httpHeader.getPayloadInputStream
            else payloadStream = payload.getInputStreamComplete

            //If we got this far, we have a new html document from clueweb to consider
            docCount += 1

            if (httpHeader != null && httpHeader.statusCode == 200) {
              docCount200 += 1

              foundEncoding(payloadStream, httpHeader.contentType) orElse utf8(payloadStream) match {
                //Success implies we were able to parse the document as either an
                case Success(html) =>
                  val doc = Option(warcRecord.header.warcRecordIdUri) match {
                    case Some(requestUri) => Jsoup.parse(html, requestUri.toString)
                    case None => Jsoup.parse(html)
                  }

                  //Our secret sauce per record
                  val trecID = Option(warcRecord.getHeader("WARC-TREC-ID").value).getOrElse(docCount.toString)
                  val title = Option(doc.title()).getOrElse("")
                  val body = Option(doc.body()).map(_.text()).getOrElse("")
                  //                  println("title", title)
                  //                  println("body", body)

                  action(trecID, title, body)

                  if (docCount % 1000 == 0) println(s"Completed $docCount documents of which $docCount200 were status 200 in ${(System.currentTimeMillis() - startTime) / 1000} seconds. Failed on $failureCount along the way :(")
                  if (docCount % 100000 == 0) println(s"Sanity test: title:$title, body:$body")
                case Failure(ex) =>
                  println(s"Failed with ${ex.getMessage}")
                  failureCount += 1
              }
            }
            gzipEntries += 1
            Option(httpHeader).foreach(_.close)
          }

          Option(payloadStream).foreach(_.close)
          Option(warcRecord).foreach(_.close)
          warcRecord = warcReader.getNextRecordFrom(in, gzipEntry.getStartOffset)
        }

        in.close()
        gzipEntry.close()
        gzipEntry = gzipReader.getNextEntry
      }
    } else {
      println("Not gz file, unexpected, not processing")
    }
  }
}
