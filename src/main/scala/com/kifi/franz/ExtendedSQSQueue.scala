package com.kifi.franz

import com.amazon.sqs.javamessaging.AmazonSQSExtendedClient
import com.amazonaws.services.sqs.model._

import scala.collection.JavaConverters._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.FiniteDuration
import scala.concurrent.{Future, blocking}

/**
  * Created by sachint on 28/7/17.
  */
abstract class ExtendedSQSQueue[T] extends SQSQueue[T] {

  override protected def sqs: AmazonSQSExtendedClient

  override def send(msg: T,
                    messageAttributes: Option[Map[String, String]],
                    delay: Option[Int]): Future[MessageId] = Future {
    blocking(sqs.sendMessage(constructSendReq(msg, messageAttributes, delay)))
  }.map(res => MessageId(res.getMessageId))


  override def sendBatch(msg: Seq[(T, Option[Map[String, String]])],
                         delay: Option[Int]): Future[(Seq[MessageId], Seq[MessageId])] = Future {
    blocking(sqs.sendMessageBatch(constructSendBatchReq(msg, delay)))
  }.map(res => {
    (res.getSuccessful.asScala.map(m => MessageId(m.getMessageId)),
      res.getFailed.asScala.map(m => MessageId(m.getId)))
  })


  override def attributes(attributeNames: Seq[String]): Future[Map[String, String]] = Future {
    blocking(sqs.getQueueAttributes(constructGetAttrReq(attributeNames)))
  }.map(res => {
    res.getAttributes.asScala.toMap
  })

  override def nextBatchRequestWithLock(requestMaxBatchSize: Int,
                                        lockTimeout: FiniteDuration,
                                        waitTimeout: FiniteDuration): Future[Seq[SQSMessage[T]]] = {
    Future {
      val req = constructNextBatchWithLockReq(requestMaxBatchSize, lockTimeout, waitTimeout)
      blocking(sqs.receiveMessage(req))
    }.map(response => {
      val rawMessages = response.getMessages
      rawMessages.asScala.map { rawMessage =>
        SQSMessage[T](
          id = MessageId(rawMessage.getMessageId),
          body = rawMessage.getBody,
          consume = { () =>
            val request = new DeleteMessageRequest
            request.setQueueUrl(queueUrl)
            request.setReceiptHandle(rawMessage.getReceiptHandle)
            Future(blocking(sqs.deleteMessage(request)))
          },
          setVisibilityTimeout = { (timeout: FiniteDuration) =>
            val request = (new ChangeMessageVisibilityRequest)
              .withQueueUrl(queueUrl)
              .withReceiptHandle(rawMessage.getReceiptHandle)
              .withVisibilityTimeout(timeout.toSeconds.toInt)
            Future(blocking(sqs.changeMessageVisibility(request)))
          },
          attributes = rawMessage.getAttributes.asScala.toMap,
          messageAttributes = rawMessage.getMessageAttributes.asScala.toMap)
      }
    })
  }

}
