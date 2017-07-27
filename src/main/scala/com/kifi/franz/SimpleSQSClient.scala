package com.kifi.franz

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.sqs.AmazonSQSAsyncClient
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient
import com.amazonaws.services.sqs.model._
import com.amazonaws.handlers.AsyncHandler
import play.api.libs.json.{Format, JsValue}

import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}
import scala.collection.JavaConversions._

class SimpleSQSClient(credentialProvider: AWSCredentialsProvider, region: Regions, buffered: Boolean) extends SQSClient {

  val _sqs = new AmazonSQSAsyncClient(credentialProvider)
  val sqs = if (buffered) new AmazonSQSBufferedAsyncClient(_sqs) else _sqs;
  sqs.setRegion(Region.getRegion(region))


  def simple(queue: QueueName, createIfNotExists: Boolean = false): SQSQueue[String] = {
    new SimpleSQSQueue(sqs, queue, createIfNotExists)
  }

  def json(queue: QueueName, createIfNotExists: Boolean = false): SQSQueue[JsValue] = {
    new JsonSQSQueue(sqs, queue, createIfNotExists)
  }


  def formatted[T](queue: QueueName, createIfNotExists: Boolean = false)(implicit format: Format[T]): SQSQueue[T] = {
    new FormattedSQSQueue(sqs, queue, createIfNotExists, format)
  }

  def delete(queue: QueueName): Future[Boolean] = {
    val queueDidExist = Promise[Boolean]
    sqs.getQueueUrlAsync(new GetQueueUrlRequest(queue.name), new AsyncHandler[GetQueueUrlRequest, GetQueueUrlResult] {
      def onError(exception: Exception) = exception match {
        case _: QueueDoesNotExistException => queueDidExist.success(false)
        case _ => queueDidExist.failure(exception)
      }

      def onSuccess(req: GetQueueUrlRequest, response: GetQueueUrlResult) = {
        val queueUrl = response.getQueueUrl()
        queueDidExist.completeWith(deleteQueueByUrl(queueUrl))
      }
    })
    queueDidExist.future
  }

  def deleteByPrefix(queuePrefix: String)(implicit executor: ExecutionContext): Future[Int] = {
    val deletedQueues = Promise[Int]
    sqs.listQueuesAsync(new ListQueuesRequest(queuePrefix), new AsyncHandler[ListQueuesRequest, ListQueuesResult] {
      def onError(exception: Exception) = deletedQueues.failure(exception)

      def onSuccess(req: ListQueuesRequest, response: ListQueuesResult) = {
        val queueUrls = response.getQueueUrls()
        Future.sequence(queueUrls.map(deleteQueueByUrl)) onComplete {
          case Failure(exception) => deletedQueues.failure(exception)
          case Success(confirmations) => deletedQueues.success(confirmations.length)
        }
      }
    })
    deletedQueues.future
  }

  private def deleteQueueByUrl(queueUrl: String): Future[Boolean] = {
    val deletedQueue = Promise[Boolean]
    val delQueueReq = new DeleteQueueRequest(queueUrl)
    val asyncHandler = new AsyncHandler[DeleteQueueRequest, DeleteQueueResult] {
      override def onError(exception: Exception): Unit = deletedQueue.failure(exception)

      override def onSuccess(request: DeleteQueueRequest, result: DeleteQueueResult): Unit =
        deletedQueue.success(true)
    }
    sqs.deleteQueueAsync(delQueueReq, asyncHandler)
    deletedQueue.future
  }

  def shutdown(): Unit = {
    this.sqs.shutdown()
  }

}

object SimpleSQSClient {

  def apply(credentials: AWSCredentials, region: Regions, buffered: Boolean = false): SQSClient = {
    val credentialProvider = new AWSCredentialsProvider {
      def getCredentials() = credentials

      def refresh() = {}
    }
    new SimpleSQSClient(credentialProvider, region, buffered);
  }

  def apply(key: String, secret: String, region: Regions): SQSClient = {
    val credentials = new AWSCredentials {
      def getAWSAccessKeyId() = key

      def getAWSSecretKey() = secret
    }
    this (credentials, region, false)
  }

}

