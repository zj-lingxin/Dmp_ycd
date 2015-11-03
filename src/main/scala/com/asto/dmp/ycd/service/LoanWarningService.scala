package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base.{Constants, DataSource}
import com.asto.dmp.ycd.util.mail.MailAgent
import com.asto.dmp.ycd.util.{FileUtils, Utils}

/**
 * 贷后预警规则
 */
class LoanWarningService extends DataSource with scala.Serializable {
  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行贷后模型"))


      FileUtils.saveAsTextFile(null, Constants.OutputPath.LOAN_WARNING_TEXT)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.LOAN_WARNING_SUBJECT).sendMessage()
        logError(Constants.Mail.LOAN_WARNING_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("贷后模型运行结束"))
    }
  }
}

