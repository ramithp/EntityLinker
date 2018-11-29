package com.boston.lti.entitylink.warc

import com.yahoo.semsearch.fastlinking.FastEntityLinker
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash
import com.yahoo.semsearch.fastlinking.view.EmptyContext
import it.unimi.dsi.fastutil.io.BinIO

import scala.concurrent.duration._
import scala.concurrent.Await
import scala.util.{Failure, Success, Try}
import io.circe.syntax._

// In order to evaluate tasks, we'll need a Scheduler
import monix.execution.Scheduler.Implicits.global

// Task is in monix.eval
import monix.eval.Task

import com.boston.lti.entitylink.EntityTagger

object FELRunner extends EntityTaggerRunner with FELTagger {
  def modelFile = args(2)
}

trait FELTagger extends EntityTagger {
  def modelFile: String

  object FEL {
    lazy val fel = {
      val hash = BinIO.loadObject(modelFile).asInstanceOf[QuasiSuccinctEntityHash]
      new FastEntityLinker(hash, new EmptyContext())
    }
  }


  override def getAnnotations(text: String) = {
    import scala.collection.JavaConversions._
    val annotations: Array[Map[String, String]] = text.split(". ")
      .flatMap { str =>
        //        FEL.fel.getResults(str, -20)

        val task: Task[java.util.List[FastEntityLinker.EntityResult]] = Task {
          FEL.fel.getResults(str, -20)
        }.timeout(10.seconds)

        val fut = task.runAsync
        Try(Await.result[java.util.List[FastEntityLinker.EntityResult]](fut, 10.seconds)) match {
          case Success(value) => value
          case Failure(exception) =>
            //Nothing really gets cancelled! This is potentially useless unless something cooler is done with it
            fut.cancel()
            println(s"FEL timed out on text:$str")
            List.empty[FastEntityLinker.EntityResult]
        }
      }.map(entityResult => Map("id" -> entityResult.id.toString,
      "text" -> entityResult.text.toString,
      "score" -> entityResult.score.toString,
      "type" -> entityResult.`type`.toString))
    (annotations.length, annotations.asJson)
  }
}
