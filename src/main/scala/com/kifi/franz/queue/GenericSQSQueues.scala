package com.kifi.franz.queue

import com.amazonaws.services.sqs.AmazonSQS
import play.api.libs.json.{Format, JsValue, Json}

import scala.language.implicitConversions


abstract class SimpleSQSQueue[Q <: AmazonSQS](val sqs: Q,
                                              val queue: QueueName,
                                              protected val createIfNotExists: Boolean = false) extends
  SQSQueue[String, Q] {

  protected implicit def asString(s: String): String = s

  protected implicit def fromString(s: String): String = s
}

abstract class JsonSQSQueue[Q <: AmazonSQS](val sqs: Q,
                                            val queue: QueueName,
                                            protected val createIfNotExists: Boolean = false) extends
  SQSQueue[JsValue, Q] {

  protected implicit def asString(json: JsValue): String = Json.stringify(json)

  protected implicit def fromString(s: String): JsValue = Json.parse(s)
}

abstract class FormattedSQSQueue[T, Q <: AmazonSQS](val sqs: Q,
                                                    val queue: QueueName,
                                                    protected val createIfNotExists: Boolean = false,
                                                    format: Format[T]
                                                   ) extends SQSQueue[T, Q] {

  protected implicit def asString(obj: T): String = Json.stringify(Json.toJson(obj)(format))

  protected implicit def fromString(s: String): T = Json.parse(s).as[T](format)
}
