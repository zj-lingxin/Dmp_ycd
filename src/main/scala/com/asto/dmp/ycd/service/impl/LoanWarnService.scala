package com.asto.dmp.ycd.service.impl

import com.asto.dmp.ycd.base.Constants
import com.asto.dmp.ycd.dao.impl.BizDao
import com.asto.dmp.ycd.service.Service
import com.asto.dmp.ycd.util.FileUtils

object LoanWarnService {
  def getLoanWarnInfo = {
    BizDao.weekOrderAmountWarn.leftOuterJoin(BizDao.moneyAmountWarn).map(t => (t._1, t._2._1, t._2._2.get)).map(t => (t._1, t._2._1, t._2._2, t._3._1, t._3._2, t._2._2.toString.toBoolean || t._3._2.toString.toBoolean))
  }
}

class LoanWarnService extends Service {
  override protected def runServices(): Unit = {
    FileUtils.saveAsTextFile(LoanWarnService.getLoanWarnInfo, Constants.OutputPath.LOAN_WARN_PATH)
  }
}
