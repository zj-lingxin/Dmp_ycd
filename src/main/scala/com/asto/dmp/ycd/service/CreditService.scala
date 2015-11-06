package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.dao.BizDao
import com.asto.dmp.ycd.service.CreditService._
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

class CreditService extends Services {

  override protected var startLog: String = "开始运行授信模型"

  override protected var endLog: String = "授信模型运行结束"

  override protected var mailSubject: String = Constants.Mail.CREDIT_SUBJECT

  override protected def runServices: Unit = FileUtils.saveAsTextFile(getAmountOfCredit, Constants.OutputPath.CREDIT)

}
