package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.dao.BizDao
import com.asto.dmp.ycd.util.mail.MailAgent
import com.asto.dmp.ycd.util.{Utils,FileUtils}

/**
 * 反欺诈规则
 */
class AntiFraudService extends DataSource with Serializable {

  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行反欺诈模型"))
      //BizDao.getOrderInfoProps(SQL().select("*").limit(10)).collect.map(a => (a(0),a(5))).foreach(println)
      BizDao.getOrderDetailsProps(SQL().select("*").limit(10)).collect.map(a => (a(0),a(5))).foreach(println)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.ANTI_FRAUD_SUBJECT).sendMessage()
        logError(Constants.Mail.ANTI_FRAUD_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("反欺诈模型运行结束"))
    }
  }
}
