package com.walmart.interview

import java.util.concurrent.TimeUnit

import org.apache.spark.HashPartitioner
import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration.Duration

//This class contains the logic to detect departments whose sales rank have fallen
//in the past 3 hours

object LaggingDepartments {
  def aggSalesToDeptHour(transactions: DStream[Transaction]): DStream[(String, Duration, BigDecimal)] = {
    //Aggregate all the sales upto hour level for each department
    // note that this just aggregates the sales within the microbatch. It doesn't keep state across microbatches
    val deptSalesByHour = transactions.map(t => ((t.department,Duration(t.orderTime.toHours, TimeUnit.HOURS)), t.amount))
      .reduceByKey(_ + _)
      .map({case ((d, h),a) => (d,h,a)})

    val totalDeptSalesByHour = deptSalesByHour.map({ case(d, h, a) => (d, (h, a))} )
      .updateStateByKey(HourlyDepartmentSales.sum _)
      .flatMap({case (d, hds) => hds.hourlyDeptSales.map({case (h,a)=>(d, h, a)})})

    totalDeptSalesByHour
  }

  def rankDepartmentsByHour(deptSaleByHour: DStream[(String, Duration, BigDecimal)]): DStream[(Duration, String, Int)] = {
    deptSaleByHour.map({case(d, h, a) => (h, (d, a))})
      .groupByKey()
      .map[(Duration, Seq[(String, Int)])](HourlyDepartmentSales.rank(_))
      .flatMap({case(h, ds) => ds.map({case(d,r)=>(h, d, r)})})
  }

  def findLaggingDepartments(rankedDepts: DStream[(Duration, String, Int)]): DStream[(String, Iterable[(Duration, Int)])] = {
    rankedDepts.map({case(h, d, r)=>(d, (h,r))})
      .groupByKey()
      .filter(HourlyDepartmentSales.filter(_))

  }

  def run(transactions: DStream[Transaction]):DStream[(String, Iterable[(Duration, Int)])] = findLaggingDepartments(rankDepartmentsByHour(aggSalesToDeptHour(transactions)))


}
