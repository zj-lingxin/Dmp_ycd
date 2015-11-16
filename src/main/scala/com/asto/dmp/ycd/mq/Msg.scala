package com.asto.dmp.ycd.mq

import com.asto.dmp.ycd.base.Constants
import com.asto.dmp.ycd.util.DateUtils
import scala.util.parsing.json.{JSONArray, JSONObject}

object MsgWrapper {
  def getJson(quotaItemName: String, msgList: List[Msg]): String = {
    new JSONObject(Map(
      "quotaItemName" -> quotaItemName,
      "propertyUuid" -> Constants.App.STORE_ID,
      "quotaItemList" -> JSONArray(for (msg <- msgList) yield matchMsgType(msg))
    )).toString()
  }

  def matchMsgType(msg: Msg) = {
    if (msg.isInstanceOf[MsgWithName]) {
      toMsgWithNameJsonObj(msg.asInstanceOf[MsgWithName])
    } else {
      toJsonObj(msg)
    }
  }

  def toJsonObj(msg: Msg): JSONObject = {
    new JSONObject(Map[String, Any]("indexFlag" -> msg.indexFlag, "quotaCode" -> msg.quotaCode, "targetTime" -> msg.targetTime, "quotaValue" -> msg.quotaValue))
  }

  def toMsgWithNameJsonObj(msg: MsgWithName): JSONObject = {
    new JSONObject(Map[String, Any]("indexFlag" -> msg.indexFlag, "quotaName" -> msg.quotaName, "quotaCode" -> msg.quotaCode, "targetTime" -> msg.targetTime, "quotaValue" -> msg.quotaValue))
  }
}

object Msg {
  def apply(quotaCode: String, quotaValue: Any, indexFlag: String = "2", targetTime: String = DateUtils.getStrDate("yyyyMM")) = {
    new Msg(quotaCode, quotaValue, indexFlag, targetTime)
  }
}

class Msg(val quotaCode: String, val quotaValue: Any, val indexFlag: String = "2", val targetTime: String = DateUtils.getStrDate("yyyyMM"))

object MsgWithName {
  def apply(quotaCode: String, quotaName: String, quotaValue: Any, indexFlag: String = "2", targetTime: String = DateUtils.getStrDate("yyyyMM")) = {
    new MsgWithName(quotaCode, quotaName, quotaValue, indexFlag, targetTime)
  }
}

class MsgWithName(override val quotaCode: String, val quotaName: String, override val quotaValue: Any, override val indexFlag: String, override val targetTime: String) extends Msg(quotaCode, quotaValue, indexFlag, targetTime)