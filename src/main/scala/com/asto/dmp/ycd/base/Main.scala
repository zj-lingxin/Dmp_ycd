package com.asto.dmp.ycd.base

import com.asto.dmp.ycd.mq.MQAgent
import com.asto.dmp.ycd.service._
import com.asto.dmp.ycd.util.{DateUtils, Utils}
import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    if(argsIsIllegal(args)) return
    useArgs(args)
    runAllServices()
    closeResources()
    printEndLogs(startTime)
  }

  private def useArgs(args: Array[String]) {
    Constants.App.STORE_ID = args(0)
    Constants.App.TIMESTAMP = args(1).toLong
    //从外部传入的是秒级别的时间戳，所以要乘以1000
    Constants.App.TODAY = DateUtils.timestampToStr(args(1).toLong * 1000, "yyyyMM/dd")
    if (args.length > 2 && args(2).toUpperCase() == "MQ") {
      Constants.App.MQ_ENABLE = true
    } else {
      Constants.App.MQ_ENABLE = false
    }
  }

  private def runAllServices() {
    new FieldsCalculationService().run()
    new ScoreService().run()
    new CreditService().run()
  }

  private def closeResources() = {
    MQAgent.close()
    Contexts.stopSparkContext()
  }

  private def argsIsIllegal(args: Array[String]) = {
    if (Option(args).isEmpty || args.length < 2) {
      logError(Utils.logWrapper("请传入程序参数: 店铺id[args(0)]、 时间戳[args(1)]、是否使用将消息发送给mq：[args(2)]:是（\"MQ\"）,否（不传或其他）"))
      true
    } else {
      false
    }
  }

  private def printRunningTime(startTime: Long) {
    logInfo(Utils.logWrapper(s"程序共运行${(System.currentTimeMillis() - startTime) / 1000}秒"))
  }

  private def printErrorLogsIfExist() {
    if (Constants.App.ERROR_LOG.toString != "") {
      logError(Utils.logWrapper(s"程序在运行过程中遇到了如下错误：${Constants.App.ERROR_LOG.toString}"))
    }
  }

  private def printEndLogs(startTime: Long): Unit = {
    printErrorLogsIfExist()
    printRunningTime(startTime: Long)
  }
}