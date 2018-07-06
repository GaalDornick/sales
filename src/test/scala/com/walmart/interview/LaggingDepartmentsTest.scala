package com.walmart.interview

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.Queue
import scala.concurrent.duration._

class LaggingDepartmentsTest extends FlatSpec with SparkTest with Matchers{
  def transactions1() = {
    def order = Transaction("1","1",416866.hours + 5.minutes + 10.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 1, upc = "AAA"),
      order.copy( amount= 2, upc = "BBB"),
      order.copy( amount= 3, upc = "CCC"),
      order.copy( amount= 4, upc = "DDD"),
      order.copy( amount= 5, upc = "EEE"))
  }
  def transactions2() = {
    def order = Transaction("2","2",416866.hours + 10.minutes + 45.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 6, upc = "sdfg"),
      order.copy( amount= 7, upc = "wertqa"),
      order.copy( amount= 8, upc = "CCfdgvasC"),
      order.copy( amount= 9, upc = "asdf"),
      order.copy( amount= 10, upc = "ewqrwrf"))
  }
  def transactions3() = {
    def order = Transaction("3","3",416866.hours + 5.minutes + 10.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 11, upc = "AAA"),
      order.copy( amount= 12, upc = "BBB"),
      order.copy( amount= 13, upc = "CCC"),
      order.copy( amount= 14, upc = "DDD"),
      order.copy( amount= 15, upc = "EEE"))
  }
  def transactions4() = {
    def order = Transaction("2","2",416866.hours + 10.minutes + 45.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 16, upc = "sdfg"),
      order.copy( amount= 17, upc = "wertqa"),
      order.copy( amount= 18, upc = "CCfdgvasC"),
      order.copy( amount= 19, upc = "asdf"),
      order.copy( amount= 20, upc = "ewqrwrf"))
  }
  def transactions5() = {
    def order = Transaction("1","1",416867.hours + 5.minutes + 10.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 8, upc = "AAA"),
      order.copy( amount= 9, upc = "BBB"),
      order.copy( amount= 10, upc = "CCC"),
      order.copy( amount= 11, upc = "DDD"),
      order.copy( amount= 12, upc = "EEE"))
  }
  def transactions6() = {
    def order = Transaction("2","2",416867.hours + 10.minutes + 45.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 13, upc = "sdfg"),
      order.copy( amount= 14, upc = "wertqa"),
      order.copy( amount= 15, upc = "CCfdgvasC"),
      order.copy( amount= 16, upc = "asdf"),
      order.copy( amount= 17, upc = "ewqrwrf"))
  }
  def transactions7() = {
    def order = Transaction("3","3",416867.hours + 5.minutes + 10.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 31, upc = "AAA"),
      order.copy( amount= 32, upc = "BBB"),
      order.copy( amount= 33, upc = "CCC"),
      order.copy( amount= 34, upc = "DDD"),
      order.copy( amount= 35, upc = "EEE"))
  }
  def transactions8() = {
    def order = Transaction("2","2",416867.hours + 10.minutes + 45.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 36, upc = "sdfg"),
      order.copy( amount= 37, upc = "wertqa"),
      order.copy( amount= 38, upc = "CCfdgvasC"),
      order.copy( amount= 39, upc = "asdf"),
      order.copy( amount= 30, upc = "ewqrwrf"))
  }
  def transactions9() = {
    def order = Transaction("1","1",416868.hours + 5.minutes + 10.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 1, upc = "AAA"),
      order.copy( amount= 2, upc = "BBB"),
      order.copy( amount= 3, upc = "CCC"),
      order.copy( amount= 4, upc = "DDD"),
      order.copy( amount= 5, upc = "EEE"))
  }
  def transactions10() = {
    def order = Transaction("2","2",416868.hours + 10.minutes + 45.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 6, upc = "sdfg"),
      order.copy( amount= 7, upc = "wertqa"),
      order.copy( amount= 8, upc = "CCfdgvasC"),
      order.copy( amount= 9, upc = "asdf"),
      order.copy( amount= 10, upc = "ewqrwrf"))
  }
  def transactions11() = {
    def order = Transaction("3","3",416868.hours + 5.minutes + 10.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 11, upc = "AAA"),
      order.copy( amount= 12, upc = "BBB"),
      order.copy( amount= 13, upc = "CCC"),
      order.copy( amount= 14, upc = "DDD"),
      order.copy( amount= 15, upc = "EEE"))
  }
  def transactions12() = {
    def order = Transaction("2","2",416868.hours + 10.minutes + 45.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 16, upc = "sdfg"),
      order.copy( amount= 17, upc = "wertqa"),
      order.copy( amount= 18, upc = "CCfdgvasC"),
      order.copy( amount= 19, upc = "asdf"),
      order.copy( amount= 20, upc = "ewqrwrf"))
  }
  def transactions13() = {
    def order = Transaction("1","1",416869.hours + 5.minutes + 10.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 8, upc = "AAA"),
      order.copy( amount= 9, upc = "BBB"),
      order.copy( amount= 10, upc = "CCC"),
      order.copy( amount= 11, upc = "DDD"),
      order.copy( amount= 12, upc = "EEE"))
  }
  def transactions14() = {
    def order = Transaction("2","2",416869.hours + 10.minutes + 45.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 13, upc = "sdfg"),
      order.copy( amount= 14, upc = "wertqa"),
      order.copy( amount= 15, upc = "CCfdgvasC"),
      order.copy( amount= 16, upc = "asdf"),
      order.copy( amount= 17, upc = "ewqrwrf"))
  }
  def transactions15() = {
    def order = Transaction("3","3",416869.hours + 5.minutes + 10.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 31, upc = "AAA"),
      order.copy( amount= 32, upc = "BBB"),
      order.copy( amount= 33, upc = "CCC"),
      order.copy( amount= 34, upc = "DDD"),
      order.copy( amount= 35, upc = "EEE"))
  }
  def transactions16() = {
    def order = Transaction("2","2",416869.hours + 10.minutes + 45.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 36, upc = "sdfg"),
      order.copy( amount= 37, upc = "wertqa"),
      order.copy( amount= 38, upc = "CCfdgvasC"),
      order.copy( amount= 39, upc = "asdf"),
      order.copy( amount= 30, upc = "ewqrwrf"))
  }

  "Lagging Department" should "aggregate data from one department in same hour" in {
    val input = ssc.queueStream(Queue(ssc.sparkContext.parallelize(transactions1 ++ transactions2)))
    val output = LaggingDepartments.aggSalesToDeptHour(input)
    var results = Array.empty[(String, Duration, BigDecimal)]
    output.foreachRDD(rdd => results = rdd.collect())
    ssc.start()
    ssc.awaitTerminationOrTimeout(3.seconds.toMillis)
    results should contain(("94", 416866 hour, BigDecimal(55)))
  }

  "Lagging Department" should "aggregate data from 2 department in same hour" in {
    val input = ssc.queueStream(Queue(ssc.sparkContext.parallelize(transactions1 ++ transactions2)
                              ,ssc.sparkContext.parallelize(transactions3 ++ transactions4)))
    val output = LaggingDepartments.aggSalesToDeptHour(input)
    var results = Array.empty[(String, Duration, BigDecimal)]
    output.foreachRDD(rdd => results = rdd.collect())
    ssc.start()
    ssc.awaitTerminationOrTimeout(3.seconds.toMillis)
    results should contain allOf(("94", 416866 hour, BigDecimal(55)), ("83", 416866 hour, BigDecimal(155)))
  }

  "Lagging Department" should "aggregate data from 2 department in differrent hour" in {
    val input = ssc.queueStream(Queue(ssc.sparkContext.parallelize(transactions1 ++ transactions2)
      ,ssc.sparkContext.parallelize(transactions3 ++ transactions4)
      ,ssc.sparkContext.parallelize(transactions5 ++ transactions6)
      ,ssc.sparkContext.parallelize(transactions7 ++ transactions8)))
    val output = LaggingDepartments.aggSalesToDeptHour(input)
    var results = Array.empty[(String, Duration, BigDecimal)]
    output.foreachRDD(rdd => results = rdd.collect())
    ssc.start()
    ssc.awaitTerminationOrTimeout(10.seconds.toMillis)
    results should contain only (("94", 416866 hour, BigDecimal(55)),("94", 416867 hour, BigDecimal(125))
      , ("83", 416866 hour, BigDecimal(155)), ("83", 416867 hour, BigDecimal(345)))
  }

  "Lagging Department" should "aggregate data from 2 departmentfor more than 3 hours" in {
    val input = ssc.queueStream(Queue(ssc.sparkContext.parallelize(transactions1 ++ transactions2)
      ,ssc.sparkContext.parallelize(transactions3 ++ transactions4)
      ,ssc.sparkContext.parallelize(transactions5 ++ transactions6)
      ,ssc.sparkContext.parallelize(transactions7 ++ transactions8)
      ,ssc.sparkContext.parallelize(transactions9 ++ transactions10)
      ,ssc.sparkContext.parallelize(transactions11 ++ transactions12)
      ,ssc.sparkContext.parallelize(transactions13 ++ transactions14)
      ,ssc.sparkContext.parallelize(transactions15 ++ transactions16)))
    val output = LaggingDepartments.aggSalesToDeptHour(input)
    var results = Array.empty[(String, Duration, BigDecimal)]
    output.foreachRDD({rdd =>
      val r = rdd.collect
      results = r
    })
    ssc.start()
    ssc.awaitTerminationOrTimeout(20.seconds.toMillis)
    results should contain only (("94", 416867 hour, BigDecimal(125)), ("94", 416868 hour, BigDecimal(55)),("94", 416869 hour, BigDecimal(125))
      , ("83", 416867 hour, BigDecimal(345)), ("83", 416868 hour, BigDecimal(155)), ("83", 416869 hour, BigDecimal(345))
    )
  }

  def deptHourlySales: Seq[(String, Duration, BigDecimal)] = {
    Seq(("94",416866 hour, BigDecimal(10000) )
    ,("83",416866 hour, BigDecimal(8000) )
    ,("94",416867 hour, BigDecimal(12000) )
    ,("83",416867 hour, BigDecimal(7000) )
    ,("44",416867 hour, BigDecimal(9000) )
    ,("83",416868 hour, BigDecimal(9000) )
    ,("44",416868 hour, BigDecimal(7000) ))
  }

  "Lagging Department" should "rank departments hourly" in {
    val input = ssc.queueStream(Queue(ssc.sparkContext.parallelize(deptHourlySales)))
    val output = LaggingDepartments.rankDepartmentsByHour(input)
    var results = Array.empty[(Duration, String, Int)]
    output.foreachRDD(rdd => results = (results ++ rdd.collect()))
    ssc.start()
    ssc.awaitTerminationOrTimeout(3.seconds.toMillis)
    results should contain only ((416866 hour, "94", 0), (416866 hour, "83", 1),(416867 hour, "94", 0), (416867 hour, "44", 1), (416867 hour, "83", 2),(416868 hour, "83", 0), (416868 hour, "44", 1))
  }

  def deptHourlyRanks: Seq[(Duration, String, Int)] = {
    List((416866 hour, "94", 0), (416866 hour, "83", 1), (416866 hour, "44", 2), (416866 hour, "78", 3)
      , (416867 hour, "83", 0), (416867 hour, "94", 1), (416867 hour, "78", 2), (416866 hour, "44", 3)
      , (416868 hour, "83", 0), (416868 hour, "78", 1), (416868 hour, "94", 2), (416868 hour, "44", 3))
  }
  ((416866 hour, "94", 0), (416866 hour, "83", 1),(416867 hour, "94", 0), (416867 hour, "44", 1), (416867 hour, "83", 2),(416868 hour, "83", 0), (416868 hour, "44", 1))

  "Lagging Department" should "find departments that have tanked in past 3 hours" in {

    val input = ssc.queueStream(Queue(ssc.sparkContext.parallelize(deptHourlyRanks)))
    val output = LaggingDepartments.findLaggingDepartments(input)
    var results = Array.empty[(String, Iterable[(Duration, Int)])]
    output.foreachRDD(rdd => results = (results ++ rdd.collect()))
    ssc.start()
    ssc.awaitTerminationOrTimeout(3.seconds.toMillis)
    results should contain only (("94", Seq((416866 hour, 0),(416867 hour, 1),(416868 hour, 2) )))
  }

  "Lagging Department" should "run all the data" in {
    val input = ssc.textFileStream("src\\test\\resources\\data")

  }

}
