package com.asto.dmp.ycd.base

import com.asto.dmp.ycd.service._
import com.asto.dmp.ycd.util.{DateUtils, Utils}
import org.apache.spark.Logging

object Main extends Logging {

  private def setLicenseNoAndTimestamp(licenseNo: String, timestamp: Long) {
    Constants.App.LICENSE_NO = licenseNo
    Constants.App.TIMESTAMP = timestamp
    Constants.App.TODAY = DateUtils.timestampToStr(timestamp, "yyyyMM/dd")
  }

  private def runAllServices {
    new DataPrepareService().run()
    new FieldsCalculationService().run()
    new ScoreService().run()
    new CreditService().run()
  }

  private def stopSparkContext = {
    Contexts.stopSparkContext()
  }

  private def argsIsIllegal(args: Array[String]) = {
    if (Option(args).isEmpty || args.length < 2) {
      logError(Utils.wrapLog("请传入程序参数: 许可证号、 时间戳"))
      true
    } else {
      false
    }
  }

  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    if(argsIsIllegal(args)) return

    setLicenseNoAndTimestamp(args(0), args(1).toLong)

    runAllServices

    stopSparkContext

    logInfo(s"程序共运行${(System.currentTimeMillis() - startTime) / 1000}秒")
  }
}