package com.kifi.franz

import play.api.libs.json.{Format, JsValue}

import scala.concurrent.{ExecutionContext, Future}

trait SQSClient {

  def simple(queue: QueueName, createIfNotExists: Boolean = false): AsyncSQSQueue[String]

  def json(queue: QueueName, createIfNotExists: Boolean = false): AsyncSQSQueue[JsValue]

  def formatted[T](queue: QueueName, createIfNotExists: Boolean = false)(implicit format: Format[T]): AsyncSQSQueue[T]

  def delete(queue: QueueName): Future[Boolean]

  def deleteByPrefix(queuePrefix: String)(implicit executor: ExecutionContext): Future[Int]

  def shutdown()
}

