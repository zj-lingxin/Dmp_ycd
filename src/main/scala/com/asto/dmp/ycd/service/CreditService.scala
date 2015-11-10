package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.dao.BizDao
import com.asto.dmp.ycd.util.{MQUtils, FileUtils, Utils}
import scala.collection.mutable.ListBuffer
import scala.util.parsing.json.JSONObject
/**
 * 授信规则
 * 授信金额=min(30万，近12月月均提货额*评分对应系数）
 * 评分对应系数
 * （600 650]	 1.5
 * （550 600]	 1.2
 * （500 550]	 1
 */
object CreditService extends org.apache.spark.Logging {
  //授信金额上限
  private val maxAmountOfCredit = 300000

  /** 计算评分对应系数 **/
  def getScoreCoefficient(score: Int) = {
    if (score <= 550) 1.0
    else if (score > 550 && score <= 600) 1.2
    else 1.5
  }

  /**
   * 授信额度结果
   * 店铺id	，近12月月均提货额	，评分，评分对应系数，授信额度
   */
  def getAmountOfCredit = {
    BizDao.payMoneyAnnAvg
      .leftOuterJoin(ScoreService.getAllScore.map(t => (t._1, t._7)))
      .map(t => (t._1, t._2._1, t._2._2.get, getScoreCoefficient(t._2._2.get.toString.toInt), Math.min(maxAmountOfCredit, Utils.retainDecimal(getScoreCoefficient(t._2._2.get) * t._2._1, 0).toLong))).cache()
  }

  /**
   * 将计算结果通过MQ发送出去
   */
  def sendMessageToMQ() {
    logInfo(Utils.logWrapper("sendMessageToMQ() start "))
    val list = ListBuffer[JSONObject]()
    //将rdd里的数据拆开转成json装入list
    val MQScore = getAmountOfCredit.map{
      t =>
      val list = ListBuffer[JSONObject]()
      val creditScore = MQUtils.getYcdJsonObject("M_PROP_CREDIT_SCORE", t._3, Constants.App.TIMESTAMP.toString, "1")
      val creditAmount = MQUtils.getYcdJsonObject("M_PROP_CREDIT_LIMIT_AMOUNT", t._5, Constants.App.TIMESTAMP.toString, "1")
      list += creditScore
      list += creditAmount
    }
    MQScore.collect().foreach(s => MQUtils.joinList(list, s.toList))
    //封装数据完成
    //~~修改MQUtils.sendData(Constants.App.STORE_ID, MQUtils.getPropByKey("queue_name_online"), list)
    MQUtils.sendData(Constants.App.STORE_ID, "XDGC_BIGDPLAT_PROP_CREDIT_TOBACCO_LISTEN_TTTTTTTTTT", list)
    logInfo(Utils.logWrapper("MQUtils.closeMQ()"))
    MQUtils.closeMQ()
    logInfo(Utils.logWrapper("sendMessageToMQ() end!"))
  }
}

class CreditService extends Service {
  override protected def runServices: Unit = {
    if (Constants.App.MQ_ENABLE) {
      CreditService.sendMessageToMQ()
    }
    FileUtils.saveAsTextFile(CreditService.getAmountOfCredit, Constants.OutputPath.CREDIT)
  }
}
