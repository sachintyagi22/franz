package com.kifi.franz

import com.amazonaws.services.sqs.AmazonSQSAsync

import scala.concurrent.Future
import scala.concurrent.duration.FiniteDuration
import scala.language.implicitConversions


trait FakeSQSQueue[T] extends AsyncSQSQueue[T] {

  val sqs: AmazonSQSAsync = null
  protected val createIfNotExists: Boolean = false
  val queue: QueueName = QueueName("fake")
  protected implicit def asString(obj: T): String = null
  protected implicit def fromString(s: String): T = null.asInstanceOf[T]

  override def initQueueUrl(): String = ""

  override def send(msg: T, messageAttributes: Option[Map[String,String]], delay: Option[Int] = None): Future[MessageId] = Future.successful(MessageId(""))

  override def nextBatchRequestWithLock(maxBatchSize: Int, lockTimeout: FiniteDuration, waitTimeout: FiniteDuration): Future[Seq[SQSMessage[T]]] =
    Future.successful(Seq.empty)

}
