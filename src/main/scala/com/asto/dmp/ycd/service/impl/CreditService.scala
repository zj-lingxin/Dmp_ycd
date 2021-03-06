package com.asto.dmp.ycd.service.impl

import com.asto.dmp.ycd.base.Constants
import com.asto.dmp.ycd.dao.impl.BizDao
import com.asto.dmp.ycd.mq.{MQAgent, MsgWrapper, Msg}
import com.asto.dmp.ycd.service.Service
import com.asto.dmp.ycd.util._

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
    BizDao.moneyAmountAnnAvg
      .leftOuterJoin(ScoreService.getAllScore.map(t => (t._1, t._7)))
      .map(t => (t._1, t._2._1, t._2._2.get, getScoreCoefficient(t._2._2.get.toString.toInt), Math.min(maxAmountOfCredit, Utils.retainDecimal(getScoreCoefficient(t._2._2.get) * t._2._1, 0).toLong))).cache()
  }

  /**
   * 将计算结果通过MQ发送出去
   */
  def sendCreditAmount() {
    val strMsgsOfAllStores = new StringBuffer()
    getAmountOfCredit.collect().foreach {
      eachStore =>
        val msgs = List(
          Msg("M_PROP_CREDIT_SCORE", eachStore._3, "1"),
          Msg("M_PROP_CREDIT_LIMIT_AMOUNT", eachStore._5, "1")
        )
        MQAgent.send(MsgWrapper.getJson("总得分和授信额度", msgs, eachStore._1))
        strMsgsOfAllStores.append(Msg.strMsgsOfAStore("总得分和授信额度", eachStore._1, msgs))
    }
    FileUtils.saveAsTextFile(strMsgsOfAllStores.toString, Constants.OutputPath.CREDIT_AMOUNT_PATH)
  }
}

class CreditService extends Service {
  override protected def runServices(): Unit = {
    CreditService.sendCreditAmount()
  }
}
