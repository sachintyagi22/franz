package com.kifi.franz

import com.amazonaws.services.sqs.AmazonSQSAsync
import play.api.libs.json.{JsValue, Json}

import scala.language.implicitConversions


class SimpleSQSQueue(val sqs: AmazonSQSAsync,
                     val queue: QueueName,
                     protected val createIfNotExists: Boolean = false) extends
  AsyncSQSQueue[String] {

  protected implicit def asString(s: String): String = s

  protected implicit def fromString(s: String): String = s
}

class JsonSQSQueue(val sqs: AmazonSQSAsync,
                   val queue: QueueName,
                   protected val createIfNotExists: Boolean = false) extends
  AsyncSQSQueue[JsValue] {

  protected implicit def asString(json: JsValue): String = Json.stringify(json)

  protected implicit def fromString(s: String): JsValue = Json.parse(s)
}
