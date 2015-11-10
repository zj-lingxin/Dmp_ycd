package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base.{Service, Constants}
import com.asto.dmp.ycd.dao.{ServiceDao, BizDao}
import com.asto.dmp.ycd.util.FileUtils

class DataPrepareService extends Service{

  override protected def runServices(): Unit = FileUtils.saveAsTextFile(ServiceDao.fullFieldsOrder(), Constants.InputPath.TEMP_ORDER)

}
