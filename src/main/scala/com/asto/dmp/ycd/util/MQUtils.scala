package com.asto.dmp.ycd.util

import com.asto.dmp.ycd.mq.SendImpl
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.{JSONArray, JSONObject}
/**
 * Created by fengt on 2015/11/6.
 */
object MQUtils extends scala.Serializable {
  val target_date = DateUtils.monthsAgo(1,"yyyymm")
  val map = Map[String, Any]()

  /**
   * 使用支付宝统计指标编码，和指标值封装成json对象
   */
  def getYcdJsonObject(code_name: String, value: Any, target_date: String, indexFlag: String): JSONObject = {
    val result = map.+("indexFlag" -> indexFlag).+("quotaCode" -> code_name).+("quotaValue" -> value).+(
      "targetTime" -> (if (target_date != null) target_date.replace("-", "") else target_date))
     new JSONObject(result)
  }

  def joinList(list: ListBuffer[JSONObject], list_1: List[JSONObject]): Unit = {
    list_1.foreach(j => list += j)
  }

  /**
   * 将支付宝用户的统计指标封装后发送 到MQ
   */
  def sendData(propertyAcc: String,propertyAccType: String, queue_name: String, list: ListBuffer[JSONObject]) {
    val originalMap = Map("propertyAcc" -> propertyAcc)
    val secondMap= originalMap.+("propertyAccType" -> propertyAccType)
    val jsonArray = JSONArray(list.toList)
    val resultMap = secondMap.+("quotaItemList" -> jsonArray)
    val json = new JSONObject(resultMap)
    //    val sender: Send = new SendImpl
    SendImpl.send(queue_name, json.toString())
  }

  //关闭MQ
  def closeMQ()={
    SendImpl.close()
  }

}
