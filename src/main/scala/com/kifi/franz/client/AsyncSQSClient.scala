package com.kifi.franz.client

import com.amazonaws.auth.{AWSCredentials, AWSCredentialsProvider}
import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.regions.{Region, Regions}
import com.amazonaws.services.sqs.buffered.AmazonSQSBufferedAsyncClient
import com.amazonaws.services.sqs.model._
import com.amazonaws.services.sqs.{AmazonSQSAsync, AmazonSQSAsyncClient}
import com.kifi.franz.queue._
import play.api.libs.json.{Format, JsValue}

import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success}

class AsyncSQSClient(credentialProvider: AWSCredentialsProvider,
                     region: Regions, buffered: Boolean) extends SQSClient[AmazonSQSAsync] {

  val _sqs = new AmazonSQSAsyncClient(credentialProvider)
  val sqs: AmazonSQSAsync = if (buffered) new AmazonSQSBufferedAsyncClient(_sqs) else _sqs
  sqs.setRegion(Region.getRegion(region))


  override def simple(queue: QueueName,
                      createIfNotExists: Boolean = false): SQSQueue[String, AmazonSQSAsync] = {
    new SimpleSQSQueue(sqs, queue, createIfNotExists) with AsyncSQSQueue[String]
  }

  override def json(queue: QueueName,
                    createIfNotExists: Boolean = false): SQSQueue[JsValue, AmazonSQSAsync] = {
    new JsonSQSQueue(sqs, queue, createIfNotExists) with AsyncSQSQueue[JsValue]
  }


  override def formatted[T](queue: QueueName, createIfNotExists: Boolean = false)
                           (implicit format: Format[T]): SQSQueue[T, AmazonSQSAsync] = {
    new FormattedSQSQueue(sqs, queue, createIfNotExists, format) with AsyncSQSQueue[T]
  }

  def delete(queue: QueueName)
            (implicit executor: ExecutionContext): Future[Boolean] = {
    val queueDidExist = Promise[Boolean]
    sqs.getQueueUrlAsync(new GetQueueUrlRequest(queue.name), new AsyncHandler[GetQueueUrlRequest, GetQueueUrlResult] {
      def onError(exception: Exception): Unit = exception match {
        case _: QueueDoesNotExistException => queueDidExist.success(false)
        case _ => queueDidExist.failure(exception)
      }

      def onSuccess(req: GetQueueUrlRequest, response: GetQueueUrlResult): Unit = {
        val queueUrl = response.getQueueUrl
        queueDidExist.completeWith(deleteQueueByUrl(queueUrl))
      }
    })
    queueDidExist.future
  }

  def deleteByPrefix(queuePrefix: String)
                    (implicit executor: ExecutionContext): Future[Int] = {
    val deletedQueues = Promise[Int]
    sqs.listQueuesAsync(new ListQueuesRequest(queuePrefix), new AsyncHandler[ListQueuesRequest, ListQueuesResult] {
      def onError(exception: Exception): Unit = deletedQueues.failure(exception)

      def onSuccess(req: ListQueuesRequest, response: ListQueuesResult): Unit = {
        val queueUrls = response.getQueueUrls.asScala
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

object AsyncSQSClient {

  def apply(credentials: AWSCredentials,
            region: Regions,
            buffered: Boolean = false): SQSClient[AmazonSQSAsync] = {
    val credentialProvider = new AWSCredentialsProvider {
      def getCredentials: AWSCredentials = credentials

      def refresh(): Unit = {}
    }
    new AsyncSQSClient(credentialProvider, region, buffered)
  }

  def apply(key: String,
            secret: String,
            region: Regions): SQSClient[AmazonSQSAsync] = {
    val credentials = new AWSCredentials {
      def getAWSAccessKeyId: String = key

      def getAWSSecretKey: String = secret
    }
    this (credentials, region)
  }

}

