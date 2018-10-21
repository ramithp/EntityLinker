package com.boston.lti.entitylink

import java.io._
import java.nio.file.{Files, Path, Paths}

import org.jwat.common._
import org.jwat.gzip.{GzipEntry, GzipReader}
import org.jwat.warc.{WarcReader, WarcReaderFactory, WarcReaderUncompressed, WarcRecord}


object WarcUtils {

  @throws[IOException]
  def listFiles(directoryName: String, files: scala.collection.mutable.ListBuffer[File] = scala.collection.mutable.ListBuffer.empty[File]): List[File] = {
    val directory = new File(directoryName)
    // get all the files from a directory
    val fList = directory.listFiles
    for (file <- fList) {
      if (file.isFile) files.append(file)
      else if (file.isDirectory) {
        listFiles(file.getAbsolutePath, files)
      }
    }
    files.toList
  }

  //Largely derived from org.jwat.tools.tasks.extract
  def processFile(filePath: String)(action: (WarcRecord, HttpHeader, InputStream) => Unit) = {
    val raf = new RandomAccessFile(filePath, "r")
    val rafin = new RandomAccessFileInputStream(raf)
    val pbin = new ByteCountingPushBackInputStream(new BufferedInputStream(rafin, 8192), 32)

    if (GzipReader.isGzipped(pbin)) {
      println("Expected format of file gz found")

      val gzipReader = new GzipReader(pbin)
      var in: InputStream = null
      var gzipEntries = 0

      //Not sure what these mean, but came from ExtractFile.java from
      val warcReader = WarcReaderFactory.getReaderUncompressed
      warcReader.setWarcTargetUriProfile(UriProfile.RFC3986_ABS_16BIT_LAX)
      warcReader.setBlockDigestEnabled(true)
      warcReader.setPayloadDigestEnabled(true)
      warcReader.setRecordHeaderMaxSize(8192)
      warcReader.setPayloadHeaderMaxSize(32768)

      var gzipEntry = gzipReader.getNextEntry
      while (gzipEntry != null) {
        in = new ByteCountingPushBackInputStream(new BufferedInputStream(gzipEntry.getInputStream, 8192), 32)
        println(gzipEntries)
        var warcRecord = warcReader.getNextRecordFrom(in, gzipEntry.getStartOffset)
        import scala.collection.JavaConverters._

        while (warcRecord != null) {
          //println(warcRecord.getHeaderList.asScala.map(_.value))
          val payload = warcRecord.getPayload
          var payloadStream: InputStream = null
          if (payload != null) {
            val httpHeader = warcRecord.getHttpHeader
            if (httpHeader != null) payloadStream = httpHeader.getPayloadInputStream
            else payloadStream = payload.getInputStreamComplete

            //Our secret sauce per tagger type
            action(warcRecord, httpHeader, payloadStream)

            gzipEntries += 1
            if (gzipEntries % 10000 == 0) println(s"Completed processing of :$gzipEntries records")
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
