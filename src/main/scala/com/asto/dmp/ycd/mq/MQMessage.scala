package com.asto.dmp.ycd.mq

import com.asto.dmp.ycd.base.Constants
import com.asto.dmp.ycd.util.DateUtils
import scala.util.parsing.json.{JSONArray, JSONObject}

object MQMessage {
  def amountOfCredit(scoreAndMoneyTuple: (Int, Long)): Map[String, Any] = {
    val listBuffer = scala.collection.mutable.ListBuffer[JSONObject](
      creditAmountJsonObj("M_PROP_CREDIT_SCORE", scoreAndMoneyTuple._1, "1"),
      creditAmountJsonObj("M_PROP_CREDIT_LIMIT_AMOUNT", scoreAndMoneyTuple._2, "1")
    )
    Map("propertyUuid" -> Constants.App.STORE_ID, "quotaItemList" -> JSONArray(listBuffer.toList))
  }

  def creditAmountJsonObj(code_name: String, value: Any, indexFlag: String = "2"): JSONObject = {
    new JSONObject(Map[String, Any]("indexFlag" -> indexFlag, "quotaCode" -> code_name, "targetTime" -> DateUtils.getStrDate("yyyyMM"), "quotaValue" -> value))
  }

  def allScore(scoreTuple: (Int, Int, Int, Int, Int, Int)): Map[String, Any] = {
    val listBuffer = scala.collection.mutable.ListBuffer[JSONObject](
      creditAmountJsonObj("M_SCALE_SCORE", scoreTuple._1),
      creditAmountJsonObj("M_PROFIT_SCORE", scoreTuple._2),
      creditAmountJsonObj("M_GROWING_UP_SCORE", scoreTuple._3),
      creditAmountJsonObj("M_OPERATION_SCORE", scoreTuple._4),
      creditAmountJsonObj("M_MARKET_SCORE", scoreTuple._5),
      creditAmountJsonObj("M_PROP_CREDIT_SCORE", scoreTuple._6)

    )
    Map("propertyUuid" -> Constants.App.STORE_ID, "quotaItemList" -> JSONArray(listBuffer.toList))
  }
}
