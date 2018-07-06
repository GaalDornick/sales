package com.walmart.interview

import java.io.File

import org.apache.spark.SparkConf
import org.apache.spark.streaming.scheduler.{StatsReportListener, StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.streaming.{Minutes, Seconds, StreamingContext}
import org.scalatest.{BeforeAndAfterEach, Suite}
import org.slf4j.LoggerFactory

trait SparkTest extends BeforeAndAfterEach{ this: Suite =>

  val logger = LoggerFactory.getLogger(this.getClass)
  private def delete(file: File) {
    if(file.exists()) {
      if (file.isDirectory)
        Option(file.listFiles).map(_.toList).getOrElse(Nil).foreach(delete(_))
      file.delete
    }
  }
  var ssc: StreamingContext = _

  override def beforeEach(): Unit = {
    super.beforeEach()
    ssc = {
      delete(new File("target/checkpoint"))
      val conf = new SparkConf().setMaster("local[2]").setAppName("WalmartSales")
      val s = new StreamingContext(conf, Seconds(1))
      s.checkpoint("target/checkpoint")
      s
    }
//    ssc.addStreamingListener(new StreamingListener {
//      override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) = {
//
//
//        logger.info(s"COmpleted ${batchCompleted.batchInfo.numRecords} records")
//        super.onBatchCompleted(batchCompleted)
//      }
//    })
    ssc.addStreamingListener(new StatsReportListener())
  }

  override def afterEach(): Unit = {
    ssc.stop()
    super.afterEach()
  }
}
