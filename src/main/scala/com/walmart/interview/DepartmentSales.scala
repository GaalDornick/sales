package com.walmart.interview

import org.apache.spark.streaming.Time

import scala.collection.immutable.SortedMap
import scala.concurrent.duration.Duration




//THis class keeps track of the sales done at a particular department in the last 3 hours
class HourlyDepartmentSales extends Serializable {
 var hourlyDeptSales : SortedMap[Duration, BigDecimal] = SortedMap.empty[Duration,BigDecimal]

  def add(hour: Duration, amount: BigDecimal): HourlyDepartmentSales = {

    hourlyDeptSales = hourlyDeptSales + (hour -> (hourlyDeptSales.getOrElse(hour,BigDecimal(0)) + amount))
    if(hourlyDeptSales.size>3) {
      // if there are more than 3 hours in the map, pop the first one out
      hourlyDeptSales = hourlyDeptSales - hourlyDeptSales.firstKey
    }
    this
  }
  def add(s:(Duration, BigDecimal)): HourlyDepartmentSales = add(s._1, s._2)

}

object HourlyDepartmentSales {


  def apply()  = new HourlyDepartmentSales
  def sum(amounts: Seq[(Duration, BigDecimal)], ohds: Option[HourlyDepartmentSales]) : Option[HourlyDepartmentSales] = {
    Some(amounts.foldLeft[HourlyDepartmentSales](ohds.getOrElse(new HourlyDepartmentSales))
                        ({(hds, s) => hds.add(s)}))
  }

  def rank[K](hourlySales: (K, Iterable[(String, BigDecimal)])):(K, Seq[(String, Int)]) = {
    val (hour, deptSales) = hourlySales
    val rankedDept: Seq[(String, Int)] = deptSales.toSeq.sortBy(_._2)
      .reverse
      .map(_._1)
      .zipWithIndex

    (hour, rankedDept)
  }

  def filter(deptRanks:(Any, Iterable[(Duration, Int)])) : Boolean = {
    val(_, hourlyRanks) = deptRanks
    if(hourlyRanks.size>=3) {
      hourlyRanks.toList.sortBy(_._1)
        .map(_._2)
        .sliding(2)
        .map(l => l.head < l.tail(0))
        .reduce(_ && _)
    } else {
      // this dept doesn't have 3 hours work of data.. chuck it out
      false
    }

  }

}

