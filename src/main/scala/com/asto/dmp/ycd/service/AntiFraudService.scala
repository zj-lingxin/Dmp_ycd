package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.util.mail.MailAgent
import com.asto.dmp.ycd.util.{Utils,FileUtils}

/**
 * 反欺诈规则
 */
class AntiFraudService extends DataSource with Serializable {

  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行反欺诈模型"))
      //输出1：订单ID, 订单日期, 餐厅ID ,餐厅名称 ,下单客户ID	,下单时间	,订单额 ,刷单指标值1	,刷单指标值2,	刷单指标值3,	刷单指标值4,	刷单指标值5,	是否刷单
      FileUtils.saveAsTextFile(null, Constants.OutputPath.ANTI_FRAUD_FAKED_RATE_TEXT)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.ANTI_FRAUD_SUBJECT).sendMessage()
        logError(Constants.Mail.ANTI_FRAUD_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("反欺诈模型运行结束"))
    }
  }
}
