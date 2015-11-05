package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base.{Constants, DataSource}
import com.asto.dmp.ycd.dao.ScoreDao
import com.asto.dmp.ycd.util.mail.MailAgent
import com.asto.dmp.ycd.util.{FileUtils, Utils}

/**
 * 贷后预警规则
 */
class ScoreService extends DataSource {
  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行评分模型"))
    val payMoneyAnnAvgGPA = ScoreDao.payMoneyAnnAvg.foreach(println)
    val perCigarAvgPriceOfAnnAvgGPA = ScoreDao.perCigarAvgPriceOfAnnAvg.foreach(println)

      //FileUtils.saveAsTextFile(null, Constants.OutputPath.LOAN_WARNING_TEXT)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.SCORE_SUBJECT).sendMessage()
        logError(Constants.Mail.SCORE_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("评分模型运行结束"))
    }
  }
}

object ScoreService {
  //规模	 订货额年均值权重 30%
  val payMoneyAnnAvgWeights = 0.3
  //规模 每条均价年均值权重 5%
  val perCigarAvgPriceOfAnnAvgWeights = 0.05

  //盈利	 销售额租金比权重 10%
  val salesRentRatioWeights = 0.1

  //盈利	毛利率权重 10%
 // val salesRentRatioWeights = 0.1
/*  盈利	10%	毛利率	1年毛利率
    成长	20%	月销售增长比	近3月平均销售/近6月平均销售
  运营	5%	订货条数年均值	平均每月订货量（条）
  运营	5%	经营期限（月）	申报月起经营月份数  减 最早一笔网上订单的月份
  运营	5%	活跃品类最近一个月的值才参与模型计算	月均活跃品类 减 基准值20种
  运营	5%	品类集中度	金额TOP10的金额占比
  市场	5%	线下商圈指数	商圈指数100%分制，默认值为80%*/

}
