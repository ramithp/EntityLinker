package com.boston.lti.entitylink

import com.yahoo.semsearch.fastlinking.{EntityContextFastEntityLinker, FastEntityLinker}
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash
import com.yahoo.semsearch.fastlinking.w2v.{CentroidEntityContext, LREntityContext}
import io.circe.syntax._
import it.unimi.dsi.fastutil.io.BinIO

import scala.concurrent.Await
import scala.concurrent.duration._
import scala.util._
import monix.execution.Scheduler.Implicits.global
import monix.eval.Task

//TODO avoid duplicate class
object SemanticLRRunner extends EntityTaggerRunner {

  private object SemanticFEL {
    val modelFile = args(2)
    val vectorsFile = args(3)
    val entitiesFile = args(4)
    val fel = {
      val hash = BinIO.loadObject(modelFile).asInstanceOf[QuasiSuccinctEntityHash]
      val queryContext = new LREntityContext(vectorsFile, entitiesFile, hash)
      new EntityContextFastEntityLinker(hash, queryContext)
    }
  }


  override def getAnnotations(text: String) = {
    import scala.collection.JavaConversions._
    val annotations: Array[Map[String, String]] = text.split(". ")
      .flatMap { str =>
        val task: Task[java.util.List[FastEntityLinker.EntityResult]] = Task {
          SemanticFEL.fel.getResults(str, -20)
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


object SemanticCentroidRunner extends EntityTaggerRunner {

  private object SemanticFEL {
    val modelFile = args(2)
    val vectorsFile = args(3)
    val entitiesFile = args(4)
    val fel = {
      val hash = BinIO.loadObject(modelFile).asInstanceOf[QuasiSuccinctEntityHash]
      val queryContext = new CentroidEntityContext(vectorsFile, entitiesFile, hash)
      new EntityContextFastEntityLinker(hash, queryContext)
    }
  }


  override def getAnnotations(text: String) = {
    import scala.collection.JavaConversions._
    val annotations: Array[Map[String, String]] = text.split(". ")
      .flatMap { str =>
        val task: Task[java.util.List[FastEntityLinker.EntityResult]] = Task {
          SemanticFEL.fel.getResults(str, -20)
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