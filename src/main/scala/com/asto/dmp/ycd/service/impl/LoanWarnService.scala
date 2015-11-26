package com.asto.dmp.ycd.service.impl

import com.asto.dmp.ycd.base.Constants
import com.asto.dmp.ycd.dao.impl.BizDao
import com.asto.dmp.ycd.mq.{Msg, MsgWrapper, MQAgent}
import com.asto.dmp.ycd.service.Service
import com.asto.dmp.ycd.util.{DateUtils, FileUtils}

object LoanWarnService {
  def sendAndSaveMsg = {
    val strMsgsOfAllStores = new StringBuffer()

    BizDao.weekOrderAmountWarn.leftOuterJoin(BizDao.moneyAmountWarn).map(t => (t._1, t._2._1, t._2._2.get)).map(t => (t._1, t._2._1,  t._3._1, t._2._2.toString.toBoolean || t._3._2.toString.toBoolean)).collect().foreach{
      eachStore =>
        val msgs = List(
          Msg("W_WEEK_ORDER_AMOUNT", eachStore._2, "2", DateUtils.getStrDate("yyyyMMdd")),
          Msg("W_WEEK_ORDER_AMOUNT_RATE", eachStore._3, "2", DateUtils.getStrDate("yyyyMMdd")),
          Msg("W_IS_WARNNING", eachStore._4, "2", DateUtils.getStrDate("yyyyMMdd"))
        )
        MQAgent.send(MsgWrapper.getJson("周预警信息", msgs, eachStore._1))
        strMsgsOfAllStores.append(Msg.strMsgsOfAStore("周预警信息", eachStore._1, msgs))
    }
    FileUtils.saveAsTextFile(strMsgsOfAllStores.toString, Constants.OutputPath.LOAN_WARN_PATH)
  }
}

class LoanWarnService extends Service {
  override protected def runServices(): Unit = {
    LoanWarnService.sendAndSaveMsg
  }
}
