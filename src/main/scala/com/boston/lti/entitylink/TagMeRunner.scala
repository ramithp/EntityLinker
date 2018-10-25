package com.boston.lti.entitylink

import it.acubelab.tagme.config.TagmeConfig
import it.acubelab.tagme._
import it.acubelab.tagme.preprocessing.TopicSearcher

import scala.collection.mutable.ListBuffer
import io.circe.syntax._

object TagMeRunner extends EntityTaggerRunner {

  object TagMe {
    val lang = "en"
    TagmeConfig.init()
    val parser = new TagmeParser(lang, true)
    val disamb = new Disambiguator(lang)
    val segmentation = new Segmentation
    val rho = new RhoMeasure
    val rel = it.acubelab.tagme.RelatednessMeasure.create(lang)
    val topicSearcher = new TopicSearcher(lang)
  }


  override def getAnnotations(text: String) = {
    var entityCounter = 0
    val annotatedText = new AnnotatedText(text)

    import TagMe._
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
    (entityCounter, annotationsBuffer.asJson)
  }
}