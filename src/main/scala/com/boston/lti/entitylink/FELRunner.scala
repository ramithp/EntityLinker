package com.boston.lti.entitylink

import com.yahoo.semsearch.fastlinking.FastEntityLinker
import com.yahoo.semsearch.fastlinking.hash.QuasiSuccinctEntityHash
import com.yahoo.semsearch.fastlinking.view.EmptyContext
import it.unimi.dsi.fastutil.io.BinIO
import io.circe.syntax._

object FELRunner extends EntityTaggerRunner {

  object FEL {
    val modelFile = args(2)

    val fel = {
      val hash = BinIO.loadObject(modelFile).asInstanceOf[QuasiSuccinctEntityHash]
      new FastEntityLinker(hash, new EmptyContext())
    }
  }

  override def getAnnotations(text: String) = {
    import scala.collection.JavaConversions._
    val annotations: Array[Map[String, String]] = text.split(". ")
      .flatMap(str => FEL.fel.getResults(str, -20))
      .map(entityResult => Map("id" -> entityResult.id.toString,
        "text" -> entityResult.text.toString,
        "score" -> entityResult.score.toString,
        "type" -> entityResult.`type`.toString))
    (annotations.length, annotations.asJson)
  }
}