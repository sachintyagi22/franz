package com.kifi.franz.queue

import com.amazonaws.services.sqs.AmazonSQS
import com.amazonaws.services.sqs.model._
import play.api.libs.iteratee.Enumerator

import scala.collection.JavaConverters._
import scala.concurrent.duration.{FiniteDuration, SECONDS}
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.language.implicitConversions

trait SQSQueue[T, +Q <: AmazonSQS] {

  import SQSQueue.DefaultWaitTimeout

  val queue: QueueName

  protected def sqs: Q

  protected val createIfNotExists: Boolean

  protected implicit def asString(obj: T): String

  protected implicit def fromString(s: String): T

  protected val queueUrl: String = initQueueUrl()

  protected def initQueueUrl(): String = {
    try {
      sqs.getQueueUrl(new GetQueueUrlRequest(queue.name)).getQueueUrl
    } catch {
      case t: com.amazonaws.services.sqs.model.QueueDoesNotExistException if createIfNotExists => sqs.createQueue(new CreateQueueRequest(queue.name)).getQueueUrl
      case t: Throwable => throw t
    }
  }

  protected def stringMessageAttribute(attributeValue: String): MessageAttributeValue = {
    val attr = new MessageAttributeValue()
    attr.setDataType("String")
    attr.setStringValue(attributeValue)
    attr
  }

  protected def loopFuture[A](f: => Future[Option[A]],
                              promise: Promise[A] = Promise[A]())
                             (implicit ec: ExecutionContext): Future[A] = {
    f.onComplete {
      case util.Success(Some(res)) => promise.success(res)
      case util.Success(None) => loopFuture(f, promise)
      case util.Failure(ex) => promise.failure(ex)
    }
    promise.future
  }

  protected def loopFutureBatch[A](f: => Future[Seq[A]],
                                   promise: Promise[Seq[A]] = Promise[Seq[A]]())
                                  (implicit ec: ExecutionContext): Future[Seq[A]] = {
    f.onComplete {
      case util.Success(res) if res.nonEmpty => promise.success(res)
      case util.Success(res) if res.isEmpty => loopFutureBatch(f, promise)
      case util.Failure(ex) => promise.failure(ex)
    }
    promise.future
  }

  def send(msg: T): Future[MessageId] = send(msg, None)

  def send(msg: T, delay: Int): Future[MessageId] = send(msg, None, Some(delay))

  def send(msg: T,
           messageAttributes: Option[Map[String, String]] = None,
           delay: Option[Int] = None): Future[MessageId]

  def sendBatch(msg: Seq[(T, Option[Map[String, String]])],
                delay: Option[Int] = None): Future[(Seq[MessageId], Seq[MessageId])]

  def attributes(attributeNames: Seq[String]): Future[Map[String, String]]

  def nextBatchRequestWithLock(requestMaxBatchSize: Int,
                               lockTimeout: FiniteDuration,
                               waitTimeout: FiniteDuration): Future[Seq[SQSMessage[T]]]

  def nextBatchWithLock(maxBatchSize: Int,
                        lockTimeout: FiniteDuration,
                        waitTimeout: FiniteDuration = DefaultWaitTimeout)
                       (implicit ec: ExecutionContext): Future[Seq[SQSMessage[T]]] = {
    val maxBatchSizePerRequest = 10
    val requiredBatchRequests = Seq.fill(maxBatchSize / maxBatchSizePerRequest)(maxBatchSizePerRequest) :+ (maxBatchSize % maxBatchSizePerRequest)
    val futureBatches = requiredBatchRequests.collect {
      case requestMaxBatchSize if requestMaxBatchSize > 0 => nextBatchRequestWithLock(requestMaxBatchSize, lockTimeout, waitTimeout)
    }
    Future.sequence(futureBatches).map { batches =>
      val messages = batches.flatten
      val distinctMessages = messages.map { message => message.id -> message }.toMap.values
      distinctMessages.toSeq
    }
  }

  def nextBatch(maxBatchSize: Int,
                waitTimeout: FiniteDuration = DefaultWaitTimeout)
               (implicit ec: ExecutionContext): Future[Seq[SQSMessage[T]]] =
    nextBatchWithLock(maxBatchSize, FiniteDuration(0, SECONDS), waitTimeout)

  def nextWithLock(lockTimeout: FiniteDuration,
                   waitTimeout: FiniteDuration = DefaultWaitTimeout)
                  (implicit ec: ExecutionContext): Future[Option[SQSMessage[T]]] =
    nextBatchRequestWithLock(1, lockTimeout, waitTimeout).map(_.headOption)

  def next(waitTimeout: FiniteDuration = DefaultWaitTimeout)
          (implicit ec: ExecutionContext): Future[Option[SQSMessage[T]]] =
    nextBatchRequestWithLock(1, FiniteDuration(0, SECONDS), waitTimeout).map(_.headOption)

  def enumerator(waitTimeout: FiniteDuration = DefaultWaitTimeout)
                (implicit ec: ExecutionContext): Enumerator[SQSMessage[T]] =
    Enumerator.repeatM[SQSMessage[T]] {
      loopFuture(next(waitTimeout)(ec))
    }

  def enumeratorWithLock(lockTimeout: FiniteDuration,
                         waitTimeout: FiniteDuration = DefaultWaitTimeout)
                        (implicit ec: ExecutionContext): Enumerator[SQSMessage[T]] =
    Enumerator.repeatM[SQSMessage[T]] {
      loopFuture(nextWithLock(lockTimeout, waitTimeout))

    }

  def batchEnumerator(maxBatchSize: Int,
                      waitTimeout: FiniteDuration = DefaultWaitTimeout)
                     (implicit ec: ExecutionContext): Enumerator[Seq[SQSMessage[T]]] =
    Enumerator.repeatM[Seq[SQSMessage[T]]] {
      loopFutureBatch(nextBatch(maxBatchSize, waitTimeout))
    }

  def batchEnumeratorWithLock(maxBatchSize: Int,
                              lockTimeout: FiniteDuration,
                              waitTimeout: FiniteDuration = DefaultWaitTimeout)
                             (implicit ec: ExecutionContext): Enumerator[Seq[SQSMessage[T]]] =
    Enumerator.repeatM[Seq[SQSMessage[T]]] {
      loopFutureBatch(nextBatchWithLock(maxBatchSize, lockTimeout, waitTimeout))
    }

  // Request contruction helper methods
  protected def constructNextBatchWithLockReq(requestMaxBatchSize: Int,
                                              lockTimeout: FiniteDuration,
                                              waitTimeout: FiniteDuration): ReceiveMessageRequest = {
    val request = new ReceiveMessageRequest
    request.setMaxNumberOfMessages(requestMaxBatchSize)
    request.setVisibilityTimeout(lockTimeout.toSeconds.toInt)
    request.setWaitTimeSeconds(waitTimeout.toSeconds.toInt)
    request.setQueueUrl(queueUrl)
    request.withMessageAttributeNames("All")
    request.withAttributeNames("All")
    request
  }

  protected def constructGetAttrReq(attributeNames: Seq[String]): GetQueueAttributesRequest = {
    val request = new GetQueueAttributesRequest()
    request.setQueueUrl(queueUrl)
    request.setAttributeNames(attributeNames.asJavaCollection)
    request
  }

  protected def constructSendReq(msg: T,
                                 messageAttributes: Option[Map[String, String]],
                                 delay: Option[Int]): SendMessageRequest = {
    val request = new SendMessageRequest
    request.setMessageBody(msg)
    request.setQueueUrl(queueUrl)
    delay.foreach { d =>
      request.setDelaySeconds(d)
    }
    // foreach on an Option unfolds Some, and skips if None
    messageAttributes.foreach { ma =>
      ma.foreach { case (k, v) =>
        request.addMessageAttributesEntry(k, stringMessageAttribute(v))
      }
    }
    request
  }

  protected def constructSendBatchReq(msg: Seq[(T, Option[Map[String, String]])],
                                      delay: Option[Int] = None): SendMessageBatchRequest = {
    val request = new SendMessageBatchRequest()
    request.setQueueUrl(queueUrl)
    val entries = msg.zipWithIndex.map { case ((message, attributes), index) =>
      val entry = new SendMessageBatchRequestEntry()
      delay.foreach(entry.setDelaySeconds(_))
      attributes.foreach(m => m.foreach { case (k, v) =>
        entry.addMessageAttributesEntry(k, stringMessageAttribute(v))
      })
      entry.setMessageBody(message)
      entry.setId(index.toString)
      entry
    }
    request.setEntries(entries.asJavaCollection)
    request
  }

}

case class QueueName(name: String)

case class MessageId(id: String)

case class SQSMessage[T](id: MessageId,
                         body: T,
                         consume: () => Unit,
                         setVisibilityTimeout: (FiniteDuration) => Unit,
                         attributes: Map[String, String],
                         messageAttributes: Map[String, MessageAttributeValue]) {

  def consume[K](block: T => K): K = {
    val returnValue = block(body)
    consume()
    returnValue
  }
}

object SQSQueue {
  val DefaultWaitTimeout = FiniteDuration(10, SECONDS)
}

