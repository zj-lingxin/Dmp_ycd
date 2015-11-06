package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base.{Constants, DataSource}
import com.asto.dmp.ycd.dao.BizDao
import com.asto.dmp.ycd.util.{FileUtils, Utils}
import com.asto.dmp.ycd.util.mail.MailAgent

class DataPrepareService extends DataSource{
  def run() {
    try {
      logInfo(Utils.wrapLog("开始运行 DataPrepareService run方法"))
      FileUtils.saveAsTextFile(BizDao.fullFieldsOrder(), Constants.InputPath.FULL_FIELDS_ORDER)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.DATA_PREPARE_SUBJECT).sendMessage()
        logError(Constants.Mail.DATA_PREPARE_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("DataPrepareService run方法运行结束"))
    }
  }
}
