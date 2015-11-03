package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base.{DataSource, Constants}
import com.asto.dmp.ycd.util.mail.{MailAgent}
import com.asto.dmp.ycd.util.{Utils, FileUtils}

/**
 * 准入规则
 */
class AccessService extends DataSource {

  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行准入模型"))

      FileUtils.saveAsTextFile(null, Constants.OutputPath.ACCESS_TEXT)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.ACCESS_SUBJECT).sendMessage()
        logError(Constants.Mail.ACCESS_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("准入模型运行结束"))
    }
  }
}
