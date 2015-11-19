package com.asto.dmp.ycd.service.impl

import com.asto.dmp.ycd.dao.impl.BizDao
import com.asto.dmp.ycd.service.Service

object LoanWarnService {
  def getLoanWarnInfo = {
    BizDao.weekOrderAmountWarn.leftOuterJoin(BizDao.moneyAmountWarn).map(t => (t._1,t._2._1,t._2._2.get)).map(t => (t._1, t._2, t._3, t._2._2.toString.toBoolean || t._3._2.toString.toBoolean))
  }
}

class LoanWarnService extends Service {
  override protected def runServices(): Unit = {
    LoanWarnService.getLoanWarnInfo
  }
}
