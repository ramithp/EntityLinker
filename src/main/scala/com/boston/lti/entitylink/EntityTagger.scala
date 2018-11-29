package com.boston.lti.entitylink

import io.circe.Json

trait EntityTagger {
  def getAnnotations(text: String): (Long, Json)
}
