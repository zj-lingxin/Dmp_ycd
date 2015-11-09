package com.asto.dmp.ycd.base

import com.asto.dmp.ycd.service._
import com.asto.dmp.ycd.util.{DateUtils, Utils}
import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()

    if(argsIsIllegal(args)) return

    licenseNoAndTimeAssignment(args)

    runAllServices()

    stopSparkContext()

    printRunningTime(startTime)
  }

  private def licenseNoAndTimeAssignment(args: Array[String]) {
    Constants.App.STORE_ID = args(0)
    Constants.App.TIMESTAMP = args(1).toLong
    Constants.App.TODAY = DateUtils.timestampToStr(args(1).toLong, "yyyyMM/dd")
  }

  private def runAllServices() {
    new DataPrepareService().run()
    new FieldsCalculationService().run()
    new ScoreService().run()
    new CreditService().run()
  }

  private def stopSparkContext() = {
    Contexts.stopSparkContext()
  }

  private def argsIsIllegal(args: Array[String]) = {
    if (Option(args).isEmpty || args.length != 2) {
      logError(Utils.wrapLog("请传入程序参数: 店铺id[args(0)]、 时间戳[args(1)]"))
      true
    } else {
      false
    }
  }

  private def printRunningTime(startTime: Long) {
    logInfo(s"程序共运行${(System.currentTimeMillis() - startTime) / 1000}秒")
  }
}