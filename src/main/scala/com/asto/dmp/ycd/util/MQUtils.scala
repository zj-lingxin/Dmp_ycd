package com.asto.dmp.ycd.util

import java.io.FileInputStream
import java.util.Properties

import com.asto.dmp.ycd.mq.SendImpl
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.{JSONArray, JSONObject}
/**
 * Created by fengt on 2015/11/6.
 */
object MQUtils extends scala.Serializable {
  //MQ相关方法
  val prop = new Properties()
  val propPath = System.getProperty("PropPath")
  val hasPropPath = Option(propPath).isDefined
  def getPropByKey(propertyKey: String): String = {
    if (hasPropPath) {
      prop.load(new FileInputStream(propPath))
      new String(prop.getProperty(propertyKey).getBytes("ISO-8859-1"), "utf-8")
    } else
      prop.getProperty(propertyKey)
  }

  
  val map = Map[String, Any]()

  /**
   * 使用支付宝统计指标编码，和指标值封装成json对象
   */
  def getYcdJsonObject(code_name: String, value: Any, target_date: String, indexFlag: String): JSONObject = {
    val result = map.+("indexFlag" -> indexFlag).+("quotaCode" -> code_name).+("quotaValue" -> value).+(
      "targetTime" -> target_date)
     new JSONObject(result)
  }

  def joinList(list: ListBuffer[JSONObject], list_1: List[JSONObject]): Unit = {
    list_1.foreach(j => list += j)
  }

  /**
   * 将支付宝用户的统计指标封装后发送 到MQ
   */
  def sendData(propertyUuid: String, queue_name: String, list: ListBuffer[JSONObject]) {

    val originalMap = Map("propertyUuid" -> propertyUuid)
    val jsonArray = JSONArray(list.toList)
    val resultMap = originalMap.+("quotaItemList" -> jsonArray)
    val json = new JSONObject(resultMap)
    //    val sender: Send = new SendImpl
    println("###################### SendImpl.send(queue_name, json.toString()) start ############")
    SendImpl.send(queue_name, json.toString())
    println("###################### SendImpl.send(queue_name, json.toString()) end ############")
  }

  //关闭MQ
  def closeMQ()={
    SendImpl.close()
  }

}
