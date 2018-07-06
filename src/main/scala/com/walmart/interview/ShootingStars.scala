package com.walmart.interview

import java.util.concurrent.TimeUnit

import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration._
import scala.concurrent.duration.Duration.DurationIsOrdered

object ShootingStars {

  def aggSalesByDept(transactions: DStream[Transaction]) = {
    //aggregate all sales for each item in the RDD to the hourly level
    val upcSalesByHour = transactions.map(t=> ((t.storeNumber, t.department, t.upc, Duration(t.orderTime.toHours, TimeUnit.HOURS)),t.amount))
      .reduceByKey(_ + _)
      .map({case((s, d, upc, h),a) => (s,d,upc,h,a)})

    //merge the sales win this RDD with all the sales in previous RDDS
    val allSalesByDept = upcSalesByHour.map(r => ((r._1, r._2), (r._3, r._4, r._5)))
      .updateStateByKey[Map[String,Map[Duration, BigDecimal]]](UpcSales.sum(_,_))
    allSalesByDept
  }

  //some items my not have any sales every hour. TO ensure that the moving percentage calculations work fine
  // we have to add records for those missing hours with amount =0
  // while we are at it, we are going to drop any data that is more that 12 hours old
  def fillInGapsAndTrim(salesByDept: DStream[((String, String),Map[String, Map[Duration, BigDecimal]])]):DStream[(String, String , String, Duration, BigDecimal)] = {
    salesByDept.map({ case (k, upcHourlySales) =>
      val fixedUpcHourlySales:Map[String, Map[Duration, BigDecimal]] = upcHourlySales.map({ case(upc, hourlySales) =>
        val lastHour = hourlySales.keys.max
        val fixedHourlySales: Map[Duration, BigDecimal] =  {for(tick <- 0 to 11) yield {
          val h = lastHour - tick.hours
          h -> hourlySales.getOrElse(h, BigDecimal(0))
        }}.toMap
        (upc, fixedHourlySales)
      })
      (k,fixedUpcHourlySales )
    }).flatMap({ case (k, upcHourlySales) =>
      upcHourlySales.flatMap({ case(upc, hourlySales) =>
        hourlySales.map({ case(h,a) =>
          (k._1, k._2, upc, h, a)
        })
      })
    })
  }

  def computePercentIncrease(sales: DStream[(String, String , String, Duration, BigDecimal)]) : DStream[(String, String, String, Duration, BigDecimal)]  = {

    sales.map[((String, String, String) , (Duration, BigDecimal))](s => ((s._1, s._2, s._3), (s._4, s. _5)))// key by store, department and upc
      .groupByKey()
      .map({case(k, hourlySale) => // compute percent sales over 6 hour window
        val saleOverWindow  = hourlySale.toList.sortBy(_._1)
        .sliding(6).toList
          .map(_.reverse)
          .map(window => (window.head._1, window.head._2, window.map(_._2).sum))
          .map(r => ((r._1, if(r._2==0) BigDecimal(0) else r._2/r._3)))
      (k, saleOverWindow)
      }).map({ case(k,percentSale) => //compute percent increase
        val percentIncrease = percentSale.sliding(2).toList
          .map(_.reverse)
          .map(window => (window.head._1, window.head._2 - window.tail(0)._2))
      (k, percentIncrease)
    }).flatMap({ case(k,percentIncrease) => //now flatten it so we have a nice flat data set to work with
      percentIncrease.map(pi => (k._1, k._2, k._3, pi._1, pi._2))
    })
  }

  def findLargestIncrease(percentIncreases: DStream[(String, String, String, Duration, BigDecimal)]) = {
    percentIncreases.map(r=> ((r._1, r._2, r._4), (r._3, r._5)))
      .groupByKey() // group by store/dept/hour
      .map({ case(k, upcPercentIncrease) =>
        val largestIncrease = upcPercentIncrease.maxBy(_._2)
      (k._1, k._2, largestIncrease._1, k._3, largestIncrease._2)

    })
  }



  //For each store / department on each hour, emit the item that had the largest relative increase in % sales to it's 6 hour average within the given store / department
  def run(transactions: DStream[Transaction]) : DStream[(String, String, String, Duration, BigDecimal)] = {



    // dummy output will otput 0 records
    findLargestIncrease(computePercentIncrease(fillInGapsAndTrim(aggSalesByDept(transactions))))
  }

}
