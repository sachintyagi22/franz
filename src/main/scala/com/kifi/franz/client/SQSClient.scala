package com.kifi.franz.client

import com.amazonaws.services.sqs.AmazonSQS
import com.kifi.franz.queue.{QueueName, SQSQueue}
import play.api.libs.json.{Format, JsValue}

import scala.concurrent.{ExecutionContext, Future}

trait SQSClient[Q <: AmazonSQS] {

  def simple(queue: QueueName,
             createIfNotExists: Boolean = false): SQSQueue[String, Q]

  def json(queue: QueueName,
           createIfNotExists: Boolean = false): SQSQueue[JsValue, Q]

  def formatted[T](queue: QueueName, createIfNotExists: Boolean = false)
                  (implicit format: Format[T]): SQSQueue[T, Q]

  def delete(queue: QueueName)(implicit executor: ExecutionContext): Future[Boolean]

  def deleteByPrefix(queuePrefix: String)(implicit executor: ExecutionContext): Future[Int]

  def shutdown()
}

