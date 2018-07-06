package com.walmart.interview

import org.scalatest.{FlatSpec, Matchers}

import scala.collection.mutable.Queue
import scala.concurrent.duration._


class ShootingStarsTest  extends FlatSpec with SparkTest with Matchers{
  def transactions11() = {

    def order = Transaction("1","1",416866.hours + 5.minutes + 10.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 1, upc = "AAA"),
      order.copy( amount= 2, upc = "BBB"),
      order.copy( amount= 3, upc = "CCC"),
      order.copy( amount= 4, upc = "DDD"),
      order.copy( amount= 5, upc = "EEE"))
  }
  def transactions21() = {
    def order = Transaction("2","2",416866.hours + 10.minutes + 45.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 6, upc = "AAA"),
      order.copy( amount= 7, upc = "BBB"),
      order.copy( amount= 8, upc = "CCC"),
      order.copy( amount= 9, upc = "DDD"),
      order.copy( amount= 10, upc = "EEE"))
  }
  def transactions12() = {

    def order = Transaction("1","1",416866.hours + 20.minutes + 34.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 2, upc = "AAA"),
      order.copy( amount= 4, upc = "BBB"),
      order.copy( amount= 6, upc = "CCC"),
      order.copy( amount= 8, upc = "DDD"),
      order.copy( amount= 10, upc = "EEE"))
  }
  def transactions22() = {
    def order = Transaction("2","2",416866.hours + 34.minutes + 23.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 12, upc = "AAA"),
      order.copy( amount= 14, upc = "BBB"),
      order.copy( amount= 16, upc = "CCC"),
      order.copy( amount= 18, upc = "DDD"),
      order.copy( amount= 20, upc = "EEE"))
  }
  def transactions31() = {

    def order = Transaction("1","1",416867.hours + 10.minutes + 45.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 11, upc = "AAA"),
      order.copy( amount= 12, upc = "BBB"),
      order.copy( amount= 13, upc = "CCC"),
      order.copy( amount= 14, upc = "DDD"),
      order.copy( amount= 15, upc = "EEE"))
  }
  def transactions41() = {
    def order = Transaction("2","2",416867.hours + 10.minutes + 45.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 16, upc = "AAA"),
      order.copy( amount= 17, upc = "BBB"),
      order.copy( amount= 18, upc = "CCC"),
      order.copy( amount= 19, upc = "DDD"),
      order.copy( amount= 20, upc = "EEE"))
  }
  def transactions32() = {

    def order = Transaction("1","1",416867.hours + 20.minutes + 12.seconds,"1","94","43", 0,"","","")
    List(order.copy( amount= 31, upc = "AAA"),
      order.copy( amount= 32, upc = "BBB"),
      order.copy( amount= 33, upc = "CCC"),
      order.copy( amount= 34, upc = "DDD"),
      order.copy( amount= 35, upc = "EEE"))
  }
  def transactions42() = {
    def order = Transaction("2","2",416867.hours + 45.minutes + 1.seconds,"1","83","43", 0,"","","")
    List(order.copy( amount= 16, upc = "AAA"),
      order.copy( amount= 37, upc = "BBB"),
      order.copy( amount= 38, upc = "CCC"),
      order.copy( amount= 39, upc = "DDD"),
      order.copy( amount= 40, upc = "EEE"))
  }

  "Shooting stars" should "aggregate data from 2 department in differrent hours" in {
    val input = ssc.queueStream(Queue(ssc.sparkContext.parallelize(transactions11 ++ transactions21)
      ,ssc.sparkContext.parallelize(transactions12 ++ transactions22)
      ,ssc.sparkContext.parallelize(transactions31 ++ transactions41)
      ,ssc.sparkContext.parallelize(transactions32 ++ transactions42)))
    val output = ShootingStars.aggSalesByDept(input)
    var results = Array.empty[((String, String),Map[String, Map[Duration, BigDecimal]])]
    output.foreachRDD(rdd => results = rdd.collect())
    output.print
    ssc.start()
    ssc.awaitTerminationOrTimeout(10.seconds.toMillis)


  }

  def salesByDept: Seq[((String, String),Map[String, Map[Duration, BigDecimal]])] = {
    List((("1", "94"), Map("AAA" -> Map(416866.hours -> 1000, 416867.hours -> 453,416868.hours -> 2834,
                                        416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                        416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                        416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834)
                          , "BBB" -> Map(416867.hours -> 453,416868.hours -> 2834,
                                        416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                        416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                        416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834,416878.hours -> 2834)
                        , "CCC" -> Map(416868.hours -> 2834,
                                      416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                      416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                      416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834,416878.hours -> 2834,416879.hours -> 2834)))
      ,(("1", "83"), Map("AAA" -> Map(416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                  416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                  416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834)
                      , "BBB" -> Map(416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                  416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                  416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834,416878.hours -> 2834)
                      , "CCC" -> Map(416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                  416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                  416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834,416878.hours -> 2834,416879.hours -> 2834)))
      ,(("1", "43"), Map("AAA" -> Map(416866.hours -> 1000, 416867.hours -> 453,416868.hours -> 2834,
                                            416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                            416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834)
                                    , "BBB" -> Map(416867.hours -> 453,416868.hours -> 2834,
                                            416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                            416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834,416878.hours -> 2834)
                                    , "CCC" -> Map(416868.hours -> 2834,
                                            416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                            416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                            416878.hours -> 2834,416879.hours -> 2834)))
      ,(("2", "94"), Map("AAA" -> Map(416856.hours -> 5000, 416866.hours -> 1000, 416867.hours -> 453,416868.hours -> 2834,
                                        416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                        416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                        416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834)
                                , "BBB" -> Map(416856.hours -> 5000, 416867.hours -> 453,416868.hours -> 2834,
                                        416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                        416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                        416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834,416878.hours -> 2834)
                                , "CCC" -> Map(416856.hours -> 5000, 416868.hours -> 2834,
                                        416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                        416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                        416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834,416878.hours -> 2834,416879.hours -> 2834)))
      ,(("2", "83"), Map("AAA" -> Map(416856.hours -> 5000, 416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                        416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                        416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834)
                                , "BBB" -> Map(416856.hours -> 5000, 416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                        416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                        416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834,416878.hours -> 2834)
                                , "CCC" -> Map(416856.hours -> 5000, 416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                        416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                        416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834,416878.hours -> 2834,416879.hours -> 2834)))
      ,(("2", "43"), Map("AAA" -> Map(416856.hours -> 5000, 416866.hours -> 1000, 416867.hours -> 453,416868.hours -> 2834,
                                        416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                        416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834)
                                , "BBB" -> Map(416856.hours -> 5000, 416867.hours -> 453,416868.hours -> 2834,
                                        416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                        416875.hours -> 1000, 416876.hours -> 453,416877.hours -> 2834,416878.hours -> 2834)
                                , "CCC" -> Map(416856.hours -> 5000, 416868.hours -> 2834,
                                        416869.hours -> 1000, 416870.hours -> 453,416871.hours -> 2834,
                                        416872.hours -> 1000, 416873.hours -> 453,416874.hours -> 2834,
                                        416878.hours -> 2834,416879.hours -> 2834)))
      )
  }

  "Shooting stars" should "be able to fill in the gaps" in {
    val input = ssc.queueStream(Queue(ssc.sparkContext.parallelize(salesByDept)))
    val output = ShootingStars.fillInGapsAndTrim(input)
    var results = Array.empty[(String, String, String, Duration, BigDecimal)]
    output.foreachRDD(rdd => results = results ++ rdd.collect())
    output.print
    ssc.start()
    ssc.awaitTerminationOrTimeout(10.seconds.toMillis)
  }

  def fixedSalesByDept: Seq[(String, String , String, Duration, BigDecimal)] = {
    List(("1", "94", "AAA", 416866.hours, 100),
      ("1", "94", "AAA", 416867.hours, 200),
      ("1", "94", "AAA", 416868.hours, 0),
      ("1", "94", "AAA", 416869.hours, 400),
      ("1", "94", "AAA", 416870.hours, 800),
      ("1", "94", "AAA", 416871.hours, 500),
      ("1", "94", "AAA", 416872.hours, 600),
      ("1", "94", "AAA", 416873.hours, 300),
      ("1", "94", "AAA", 416874.hours, 400),
      ("1", "94", "AAA", 416875.hours, 800),
      ("1", "94", "AAA", 416876.hours, 200),
      ("1", "94", "AAA", 416877.hours, 600),
      ("1", "83", "AAA", 416866.hours, 500),
      ("1", "83", "AAA", 416867.hours, 300),
      ("1", "83", "AAA", 416868.hours, 200),
      ("1", "83", "AAA", 416869.hours, 300),
      ("1", "83", "AAA", 416870.hours, 400),
      ("1", "83", "AAA", 416871.hours, 0),
      ("1", "83", "AAA", 416872.hours, 1000),
      ("1", "83", "AAA", 416873.hours, 500),
      ("1", "83", "AAA", 416874.hours, 400),
      ("1", "83", "AAA", 416875.hours, 600),
      ("1", "83", "AAA", 416876.hours, 800),
      ("1", "83", "AAA", 416877.hours, 900))
  }

  "Shooting stars" should "be able to compute percent sales" in {
    val input = ssc.queueStream(Queue(ssc.sparkContext.parallelize(fixedSalesByDept)))
    val output = ShootingStars.computePercentIncrease(input)
//    var results = Array.empty[((String, String, String), (Duration, BigDecimal))]
//    output.foreachRDD(rdd => results = results ++ rdd.collect())
    output.print
    ssc.start()
    ssc.awaitTerminationOrTimeout(10.seconds.toMillis)
  }

}
