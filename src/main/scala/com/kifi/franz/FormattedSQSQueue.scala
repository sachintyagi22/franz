package com.kifi.franz

import com.amazonaws.services.sqs.AmazonSQSAsync
import play.api.libs.json.{Format, Json}

import scala.language.implicitConversions

class FormattedSQSQueue[T](
                            val sqs: AmazonSQSAsync,
                            val queue: QueueName,
                            protected val createIfNotExists: Boolean = false,
                            format: Format[T]
                          ) extends AsyncSQSQueue[T] {

  protected implicit def asString(obj: T): String = Json.stringify(Json.toJson(obj)(format))

  protected implicit def fromString(s: String): T = Json.parse(s).as[T](format)
}



