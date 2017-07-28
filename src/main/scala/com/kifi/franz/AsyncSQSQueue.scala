package com.kifi.franz

import com.amazonaws.handlers.AsyncHandler
import com.amazonaws.services.sqs.AmazonSQSAsync
import com.amazonaws.services.sqs.model._

import scala.collection.JavaConverters._
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, Promise}
import scala.language.implicitConversions

abstract class AsyncSQSQueue[T] extends SQSQueue[T] {

  override def sqs: AmazonSQSAsync

  override def send(msg: T, messageAttributes: Option[Map[String, String]] = None, delay: Option[Int] = None): Future[MessageId] = {
    val request: SendMessageRequest = constructSendReq(msg, messageAttributes, delay)
    val p = Promise[MessageId]()
    sqs.sendMessageAsync(request, new AsyncHandler[SendMessageRequest, SendMessageResult] {
      def onError(exception: Exception) = p.failure(exception)

      def onSuccess(req: SendMessageRequest, res: SendMessageResult) = p.success(MessageId(res.getMessageId))
    })
    p.future
  }

  override def sendBatch(msg: Seq[(T, Option[Map[String, String]])],
                         delay: Option[Int] = None): Future[(Seq[MessageId], Seq[MessageId])] = {
    val request: SendMessageBatchRequest = constructSendBatchReq(msg)
    val p = Promise[(Seq[MessageId], Seq[MessageId])]()
    sqs.sendMessageBatchAsync(request, new AsyncHandler[SendMessageBatchRequest, SendMessageBatchResult] {
      def onError(exception: Exception): Unit = p.failure(exception)

      def onSuccess(req: SendMessageBatchRequest, res: SendMessageBatchResult): Unit =
        p.success((res.getSuccessful.asScala.map(m => MessageId(m.getMessageId)), res.getFailed.asScala.map(m => MessageId(m.getId))))
    })
    p.future
  }

  override def attributes(attributeNames: Seq[String]): Future[Map[String, String]] = {
    val request: GetQueueAttributesRequest = constructGetAttrReq(attributeNames)

    val p = Promise[Map[String, String]]()
    sqs.getQueueAttributesAsync(request, new AsyncHandler[GetQueueAttributesRequest, GetQueueAttributesResult] {
      def onError(exception: Exception): Unit = p.failure(exception)

      def onSuccess(req: GetQueueAttributesRequest, response: GetQueueAttributesResult): Unit = {
        try {
          val rawMessages = response.getAttributes
          p.success(rawMessages.asScala.toMap)
        } catch {
          case t: Throwable => p.failure(t)
        }
      }
    })
    p.future
  }

  override def nextBatchRequestWithLock(requestMaxBatchSize: Int, lockTimeout: FiniteDuration, waitTimeout: FiniteDuration): Future[Seq[SQSMessage[T]]] = {
    val request: ReceiveMessageRequest = constructNextBatchWithLockReq(requestMaxBatchSize, lockTimeout, waitTimeout)

    val p = Promise[Seq[SQSMessage[T]]]()
    sqs.receiveMessageAsync(request, new AsyncHandler[ReceiveMessageRequest, ReceiveMessageResult] {
      def onError(exception: Exception): Unit = p.failure(exception)

      def onSuccess(req: ReceiveMessageRequest, response: ReceiveMessageResult): Unit = {
        try {
          val rawMessages = response.getMessages
          p.success(rawMessages.asScala.map { rawMessage =>
            SQSMessage[T](
              id = MessageId(rawMessage.getMessageId),
              body = rawMessage.getBody,
              consume = { () =>
                val request = new DeleteMessageRequest
                request.setQueueUrl(queueUrl)
                request.setReceiptHandle(rawMessage.getReceiptHandle)
                sqs.deleteMessageAsync(request)
              },
              setVisibilityTimeout = { (timeout: FiniteDuration) =>
                val request = (new ChangeMessageVisibilityRequest)
                  .withQueueUrl(queueUrl)
                  .withReceiptHandle(rawMessage.getReceiptHandle)
                  .withVisibilityTimeout(timeout.toSeconds.toInt)
                sqs.changeMessageVisibilityAsync(request)
              },
              attributes = rawMessage.getAttributes.asScala.toMap,
              messageAttributes = rawMessage.getMessageAttributes.asScala.toMap)
          })
        } catch {
          case t: Throwable => p.failure(t)
        }
      }
    })
    p.future
  }

}

