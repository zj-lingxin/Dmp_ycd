package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base.{Constants, DataSource}
import com.asto.dmp.ycd.util.mail.MailAgent
import com.asto.dmp.ycd.util.Utils

/**
 * Created by lingx on 2015/11/06.
 */
trait Services extends DataSource {
  protected var startLog: String
  protected var endLog: String
  protected var mailSubject: String

  protected def handlingExceptions(t: Throwable) {
    MailAgent(t, mailSubject).sendMessage()
    logError(mailSubject, t)
  }

  private def printStartLogIfDefined = if (Option(startLog).isDefined) logInfo(Utils.wrapLog(startLog))

  private def printEndLogIfDefined = if (Option(endLog).isDefined) logInfo(Utils.wrapLog(endLog))

  protected def runServices: Unit

  def run() {
    try {
      printStartLogIfDefined
      runServices
    } catch {
      case t: Throwable => handlingExceptions(t)
    } finally {
      printEndLogIfDefined
    }
  }
}
