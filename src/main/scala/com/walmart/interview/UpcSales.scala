package com.walmart.interview

import scala.concurrent.duration.Duration

object UpcSales {
  def sum(upcSales: Seq[(String, Duration, BigDecimal)], existingState: Option[Map[String, Map[Duration, BigDecimal]]]):Option[Map[String, Map[Duration, BigDecimal]]] = {

    val updatedState = upcSales.foldLeft(existingState.getOrElse(Map.empty[String, Map[Duration, BigDecimal]]))({ (state, r) =>
      val(upc, hour, amount) = r
      val upcState = state.getOrElse(upc, Map.empty[Duration, BigDecimal])
      val updateUpcState = upcState + (hour -> (upcState.getOrElse(hour, BigDecimal(0)) + amount))
      state + (upc -> updateUpcState )
    })

    Some(updatedState)
  }

}
