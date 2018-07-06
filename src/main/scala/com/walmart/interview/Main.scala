package com.walmart.interview

import java.lang
import java.util.Currency
import java.util.concurrent.TimeUnit

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.{Minutes, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.duration.Duration
//{"schema":{"type":"string","optional":false},"payload":"{\"id\":\"07965132945028730973-0000000000000000046640-5\",\"order_id\":\"07965132945028730973\",\"order_time\":\"1500766028000\",\"store_number\":\"2222\",\"department\":\"94\",\"register\":\"18\",\"amount\":\"1.48\",\"upc\":\"0000000000000000046640\",\"name\":\"TOMATO VINE\",\"description\":\"TOM OTV HM\"}"}\]
//{"id":"44098873602538434607-0000000000715756200020-1","order_id":"44098873602538434607","order_time":"1500720834000","store_number":"2222","department":"94","register":"43","amount":"1.98","upc":"0000000000715756200020","name":"STRAWBERRIES","description":"STRAWBERRY 1"}




object Main {


  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[2]").setAppName("WalmartSales")
    val ssc = new StreamingContext(conf, Minutes(1))
    ssc.checkpoint("target/checkpoint")

    val stream: DStream[String] = readSalesStream(ssc).map(_.value())

    val(transactions, badRecords) = TransactionParser.parseTextStream(stream)

    LaggingDepartments.run(transactions).print()
    ShootingStars.run(transactions).print()
    RegisterTracker.run(transactions).print()

    ssc.start()             // Start the computation
    ssc.awaitTermination()  // Wait for the computation to terminate
  }



  private def readSalesStream(ssc: StreamingContext): InputDStream[ConsumerRecord[String, String]] = {
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "192.168.99.100:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "walmartsales",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: lang.Boolean)
    )

    val topics = Array("walmartsales")
    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,
      Subscribe[String, String](topics, kafkaParams)
    )
    stream
  }

}