package com.kifi.franz.client

import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.regions.Regions
import com.amazonaws.services.sqs.AmazonSQSClient
import com.amazonaws.services.sqs.model._
import com.kifi.franz.queue._
import play.api.libs.json.{Format, JsValue}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, blocking}

/**
  * Created by sachint on 29/7/17.
  */
class ExtendedSQSClient(credentialProvider: AWSCredentialsProvider,
                        region: Regions,
                        buffered: Boolean) extends SQSClient[AmazonSQSExtendedClient] {

  private val sqs = new AmazonSQSExtendedClient(new AmazonSQSClient(credentialProvider))

  override def simple(queue: QueueName,
                      createIfNotExists: Boolean): SQSQueue[String, AmazonSQSExtendedClient] = {
    new SimpleSQSQueue(sqs, queue, createIfNotExists) with ExtendedSQSQueue[String]
  }

  override def json(queue: QueueName,
                    createIfNotExists: Boolean): SQSQueue[JsValue, AmazonSQSExtendedClient] = {
    new JsonSQSQueue(sqs, queue, createIfNotExists) with ExtendedSQSQueue[JsValue]
  }

  override def formatted[T](queue: QueueName, createIfNotExists: Boolean)
                           (implicit format: Format[T]): SQSQueue[T, AmazonSQSExtendedClient] = {
    new FormattedSQSQueue[T, AmazonSQSExtendedClient](sqs, queue, createIfNotExists, format) with ExtendedSQSQueue[T]
  }

  override def delete(queue: QueueName)
                     (implicit executor: ExecutionContext): Future[Boolean] = Future {
    blocking(sqs.getQueueUrl(new GetQueueUrlRequest(queue.name)))
  }.flatMap(res =>
    deleteQueueByUrl(res.getQueueUrl)
  ).recover {
    case _: QueueDoesNotExistException => false
  }

  override def deleteByPrefix(queuePrefix: String)
                             (implicit executor: ExecutionContext): Future[Int] = Future {
    blocking(sqs.listQueues(new ListQueuesRequest(queuePrefix)))
  }.flatMap(res =>
    Future.sequence(res.getQueueUrls.asScala.map(deleteQueueByUrl)).map(res => res.length)
  )


  private def deleteQueueByUrl(queueUrl: String)
                              (implicit executor: ExecutionContext): Future[Boolean] = Future {
    blocking(sqs.deleteQueue(new DeleteQueueRequest(queueUrl)))
  }.map(res => true)

  def shutdown(): Unit = this.sqs.shutdown()

}
