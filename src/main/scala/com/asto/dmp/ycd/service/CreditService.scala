package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.util.mail.MailAgent
import com.asto.dmp.ycd.util.{Utils, FileUtils}

/**
 * 授信规则
 */
class CreditService extends DataSource with scala.Serializable {

  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行授信模型"))
      //输出文件的字段：餐厅id, 餐厅名称, 营业额加权环比增长率, 日均净营业额, 贷款倍率, 近12个月日营业额均值, 刷单率, 授信额度, 是否准入
      FileUtils.saveAsTextFile(null, Constants.OutputPath.CREDIT_TEXT)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.CREDIT_SUBJECT).sendMessage()
        logError(Constants.Mail.CREDIT_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("授信模型运行结束"))
    }
  }
}
