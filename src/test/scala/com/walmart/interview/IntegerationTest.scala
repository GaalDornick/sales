package com.walmart.interview

import java.nio.file.{Files, Paths}

import org.apache.commons.io.FileUtils
import org.apache.spark.streaming.StreamingContext
import org.scalatest.{FlatSpec, Matchers}
import org.slf4j.LoggerFactory

import scala.concurrent.duration._
import org.json4s._

class IntegerationTest extends FlatSpec with SparkTest with Matchers{


  "Walmart Sales" should "find lagging department" in {
    val testDataPath = Paths.get("target", "data")
    FileUtils.deleteDirectory(testDataPath.toFile)
    Files.createDirectory(testDataPath)
    val results = IntegerationTest.runLaggingDept(ssc)
    ssc.stop()
  }

  "Walmart Sales" should "find shooting stars" in {
    val testDataPath = Paths.get("target", "data")
    FileUtils.deleteDirectory(testDataPath.toFile)
    Files.createDirectory(testDataPath)
    val results = IntegerationTest.runShootingStars(ssc)
    ssc.stop()
  }

  "Walmart Sales" should "get register status" in {
    val testDataPath = Paths.get("target", "data")
    FileUtils.deleteDirectory(testDataPath.toFile)
    Files.createDirectory(testDataPath)
    val results = IntegerationTest.runRegisterTracker(ssc)
    ssc.stop()
  }

}

object IntegerationTest {
  val logger = LoggerFactory.getLogger(getClass)
  def runLaggingDept(ssc: StreamingContext) = {
    val input = ssc.textFileStream("target\\data")
    var inputs = Array.empty[String]
    input.foreachRDD({ r =>
      inputs = inputs ++ r.collect()
      logger.info(s"Received ${inputs.length} records")
    })
    //    input.print()

    val (transactions, badRecords) = TransactionParser.parseTextStream(input)
    val output = LaggingDepartments.run(transactions)
    var results = Array.empty[(String, Iterable[(Duration, Int)])]

    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    output.map(r => ("department" -> r._1) ~ ("ranks" -> r._2.map(rank => ("hour" -> rank._1.toString)~("rank" -> rank._2))))
      .map(r => compact(render(r)))
      .saveAsTextFiles("target\\output\\laggers")
    output.foreachRDD({rdd =>
      results = rdd.collect()
      logger.info(s"Output ${results.length} records")
    })

    ssc.start()
    Thread.sleep(1000)
    FileUtils.copyFileToDirectory(Paths.get("src", "test", "resources", "data", "sales.json").toFile, Paths.get("target", "data").toFile, false)
    while(results.length<23) Thread.sleep(1000)
    // ssc.awaitTerminationOrTimeout(30.minutes.toMillis)
    results
  }
  def runShootingStars(ssc: StreamingContext) = {
    val input = ssc.textFileStream("target\\data")
    var inputs = Array.empty[String]
    input.foreachRDD({ r =>
      inputs = inputs ++ r.collect()
      logger.info(s"Received ${inputs.length} records")
    })
    //    input.print()

    val (transactions, badRecords) = TransactionParser.parseTextStream(input)
    val output = ShootingStars.run(transactions)

    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    output.map(r => ("store" -> r._1) ~ ("department" -> r._2) ~("upc" -> r._3) ~ ("hour" -> r._4.toString) ~ ("pctIncrease" -> r._5))
      .map(r => compact(render(r)))
      .saveAsTextFiles("target\\output\\shootingstars")

    var results = Array.empty[(String, String, String, Duration, BigDecimal)]
    output.foreachRDD({rdd =>
      results = rdd.collect()
      logger.info(s"Output ${results.length} records")
    })

    ssc.start()
    Thread.sleep(1000)
    FileUtils.copyFileToDirectory(Paths.get("src", "test", "resources", "data", "sales.json").toFile, Paths.get("target", "data").toFile, false)
    while(results.length<5398) Thread.sleep(1000)
    // ssc.awaitTerminationOrTimeout(30.minutes.toMillis)
    results
  }
  def runRegisterTracker(ssc: StreamingContext) = {
    val input = ssc.textFileStream("target\\data")
    var inputs = Array.empty[String]
    input.foreachRDD({ r =>
      inputs = inputs ++ r.collect()
      logger.info(s"Received ${inputs.length} records")
    })
    //    input.print()

    val (transactions, badRecords) = TransactionParser.parseTextStream(input)
    val output = RegisterTracker.run(transactions)

    import org.json4s.JsonDSL._
    import org.json4s.jackson.JsonMethods._

    output.map(r => ("store" -> r._1)  ~("register" -> r._2) ~ ("state" -> r._3.map(s=>("time" -> s._1.toMillis) ~ ("active" -> s._2))))
      .map(r => compact(render(r)))
      .saveAsTextFiles("target\\output\\registertracker")

    var resultCount: Long = 0
    output.foreachRDD({rdd =>
      resultCount = rdd.count()
      logger.info(s"Output $resultCount records")
    })

    ssc.start()
    Thread.sleep(1000)
    FileUtils.copyFileToDirectory(Paths.get("src", "test", "resources", "data", "sales.json").toFile, Paths.get("target", "data").toFile, false)
    while(resultCount<80) Thread.sleep(1000)
    // ssc.awaitTerminationOrTimeout(30.minutes.toMillis)

  }
}
