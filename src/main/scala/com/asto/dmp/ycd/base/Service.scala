package com.asto.dmp.ycd.base

import com.asto.dmp.ycd.util.Utils
import com.asto.dmp.ycd.util.mail.MailAgent

trait Service extends DataSource {

  protected var mailSubject: String = s"${getClass.getSimpleName}的run()方法出现异常"

  protected def handlingExceptions(t: Throwable) {
    MailAgent(t, mailSubject).sendMessage()
    logError(mailSubject, t)
  }

  protected def printStartLog = logInfo(Utils.wrapLog(s"开始运行${getClass.getSimpleName}的run()方法"))

  protected def printEndLog = logInfo(Utils.wrapLog(s"${getClass.getSimpleName}的run()方法运行结束"))

  protected def runServices

  def run() {
    try {
      printStartLog
      runServices
    } catch {
      case t: Throwable => handlingExceptions(t)
    } finally {
      printEndLog
    }
  }
}
