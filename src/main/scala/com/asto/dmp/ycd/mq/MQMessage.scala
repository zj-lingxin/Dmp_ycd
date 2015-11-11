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
    Map("propertyUuid" -> Constants.App.STORE_ID, "sendTime" -> DateUtils.getStrDate("yyyy-MM-dd HH:mm:ss"), "quotaItemList" -> JSONArray(listBuffer.toList))
  }

  def creditAmountJsonObj(code_name: String, value: Any, indexFlag: String): JSONObject = {
    new JSONObject(Map[String, Any]("indexFlag" -> indexFlag, "quotaCode" -> code_name, "quotaValue" -> value))
  }
}
