package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base.Constants
import com.asto.dmp.ycd.dao.BizDao
import com.asto.dmp.ycd.util.FileUtils

class DataPrepareService extends Services{

  override protected def runServices: Unit = FileUtils.saveAsTextFile(BizDao.fullFieldsOrder(), Constants.InputPath.FULL_FIELDS_ORDER)

  override protected var startLog: String = "开始运行 DataPrepareService"

  override protected var endLog: String = "DataPrepareService运行结束"

  override protected var mailSubject: String = Constants.Mail.DATA_PREPARE_SUBJECT

}
