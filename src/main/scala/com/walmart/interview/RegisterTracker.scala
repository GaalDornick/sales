package com.walmart.interview

import org.apache.spark.streaming.dstream.DStream

import scala.concurrent.duration._

object RegisterTracker {

  //This function gets the timestamp of each scan done by the register
  def registerActivity(transactions: DStream[Transaction]) : DStream[(String, String, Seq[Duration])] = {
    transactions.map(t => ((t.storeNumber, t.register), t.orderTime)) //extract only the data that we need
      .updateStateByKey[Seq[Duration]]({ (orderTimes: Seq[Duration], existingOrderTimes: Option[Seq[Duration]]) =>
        Some(existingOrderTimes.getOrElse(Seq.empty[Duration]) ++ orderTimes)// mere all the orderTimes across microbatches into one list
    }).map({case(k, a) => (k._1, k._2, a)}) // flatten the key

  }

  //This function analyzes all the order times for each register and deduces when the register was active and when it was inactive
  // by definition, a register is assumed to be inactive if there was more than 10 mins between 2 orders
  def detectInactivity(activity:DStream[(String, String, Seq[Duration])] ) : DStream[(String, String, Seq[(Duration, Boolean)])] = {
    // so how we gonna do this is a) sort all the order times b) compare consecitive order times.
    // If we find consecutive order times to be greater than 10 minutes, we are going to output 2 entries, one which says
    // that the register went inactive at the former time, and another that says it went active at the latter time

    activity.map[(String, String, Seq[(Duration, Boolean)])]({ case(store, register, orderTimes) =>
      val sortedOrderTimes = orderTimes.sorted // first sort it
      val stateChanges =
        sortedOrderTimes // first sort it
        .sliding(2).toSeq // put consecutive ordertimes next to each other
        .filter(o => (o.reverse.head - o.head) > 10.minutes)
        .flatMap({ o=>
          List((o.head, false), (o.reverse.head, true))
        })
      (store, register, stateChanges)
    })
  }

  def run(transactions: DStream[Transaction]) : DStream[(String, String, Seq[(Duration, Boolean)])] = {
    //dummy data
    detectInactivity(registerActivity(transactions))
  }
}
