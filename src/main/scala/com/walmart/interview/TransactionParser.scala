package com.walmart.interview

import java.util.concurrent.TimeUnit

import org.apache.spark.streaming.dstream.DStream
import org.slf4j.LoggerFactory

import scala.concurrent.duration.Duration

case class RawTransaction(id: String, order_id: String, order_time: String, store_number: String, department: String, register: String, amount: String, upc: String, name: String, description: String)
case class Transaction(id: String, orderId: String, orderTime: Duration, storeNumber: String, department: String, register: String, amount: BigDecimal, upc: String, name: String, description: String)

case class BadRecord(record: String, error: String)

object Transaction {
  def apply(raw: RawTransaction): Transaction = {
    Transaction(raw.id, raw.order_id, Duration(raw.order_time.toLong, TimeUnit.MILLISECONDS), raw.store_number, raw.department, raw.register, BigDecimal(raw.amount), raw.upc, raw.name, raw.description)
  }
}
object TransactionParser {


  val logger = LoggerFactory.getLogger(this.getClass)

  def parseTextStream(textStream: DStream[String]): (DStream[Transaction], DStream[BadRecord]) = {
    val parsedRecords = textStream.map(record => parseRecordIntoTransaction(record))
    val transactions = parsedRecords.filter(_.isLeft).map(_.left.get)
    val badRecords = parsedRecords.filter(_.isRight).map(_.right.get)
    (transactions, badRecords)
  }
  def parseRecordIntoTransaction(record: String): Either[Transaction, BadRecord] = {
    import org.json4s._

    import org.json4s.jackson.JsonMethods._

    try {
      implicit val formats = DefaultFormats

      val transaction: Transaction = Transaction(parse(record).extract[RawTransaction])
      Left(transaction)
    } catch {

      case e: Exception => {
        logger.warn("Error while parsing payload", e)
        Right(BadRecord(record, e.getMessage))
      }
    }
  }
}
