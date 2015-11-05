package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.dao.BizDao
import com.asto.dmp.ycd.service.CreditService._
import com.asto.dmp.ycd.util.mail.MailAgent
import com.asto.dmp.ycd.util.{FileUtils, Utils}

/**
 * 授信规则
 * 授信金额=min(30万，近12月月均提货额*评分对应系数）
 * 评分对应系数
 * （600 650]	 1.5
 * （550 600]	 1.2
 * （500 550]	 1
 */
object CreditService {
  //授信金额上限
  private val maxAmountOfCredit = 300000

  /** 计算评分对应系数 **/
  def getScoreCoefficient(score: Int) = {
    if (score <= 550) 1.0
    else if (score > 550 && score <= 600) 1.2
    else 1.5
  }

  def getAmountOfCredit = {
    BizDao.payMoneyAnnAvg
      .leftOuterJoin(ScoreService.getAllScore.map(t => (t._1, t._7)))
      .map(t => (t._1, t._2._1, t._2._2.get, getScoreCoefficient(t._2._2.get.toString.toInt), Math.min(maxAmountOfCredit, Utils.retainDecimal(getScoreCoefficient(t._2._2.get) * t._2._1, 0).toLong)))
  }
}

class CreditService extends DataSource {

  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行授信模型"))
      FileUtils.saveAsTextFile(getAmountOfCredit, Constants.OutputPath.CREDIT)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.CREDIT_SUBJECT).sendMessage()
        logError(Constants.Mail.CREDIT_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("授信模型运行结束"))
    }
  }
}
