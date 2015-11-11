package com.asto.dmp.ycd.mq

import com.asto.dmp.ycd.base.Constants
import com.asto.dmp.ycd.util.DateUtils
import scala.util.parsing.json.{JSONArray, JSONObject}

object Msg {
  def getJson(quotaItemName: String, msgList: List[Msg]): String = {
    new JSONObject(Map(
      "quotaItemName" -> quotaItemName,
      "propertyUuid" -> Constants.App.STORE_ID,
      "quotaItemList" -> JSONArray(for (msg <- msgList) yield toJsonObj(msg))
    )).toString()
  }

  def toJsonObj(msg: Msg): JSONObject = {
    new JSONObject(Map[String, Any]("indexFlag" -> msg.indexFlag, "quotaCode" -> msg.quotaCode, "targetTime" -> msg.targetTime, "quotaValue" -> msg.quotaValue))
  }
}

case class Msg(var quotaCode: String, var quotaValue: Any, var indexFlag: String = "2", var targetTime: String = DateUtils.getStrDate("yyyyMM"))