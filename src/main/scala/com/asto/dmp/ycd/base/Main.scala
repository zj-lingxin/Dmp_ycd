package com.asto.dmp.ycd.base

import com.asto.dmp.ycd.mq.MQAgent
import com.asto.dmp.ycd.service.impl.{ScoreService, FieldsCalculationService, CreditService}
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

  /**
   * 对传入的参数进行赋值
   */
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

  /**
   * 运行所有的模型
   */
  private def runAllServices() {
    new FieldsCalculationService().run()
    new ScoreService().run()
    new CreditService().run()
  }

  /**
   * 关闭用到的资源
   */
  private def closeResources() = {
    MQAgent.close()
    Contexts.stopSparkContext()
  }


  /**
   * 判断传入的参数是否合法
   */
  private def argsIsIllegal(args: Array[String]) = {
    if (Option(args).isEmpty || args.length < 2) {
      logError(Utils.logWrapper("请传入程序参数: 店铺id[args(0)]、 时间戳[args(1)]、是否使用将消息发送给mq：[args(2)]:是（\"MQ\"）,否（不传或其他）"))
      true
    } else {
      false
    }
  }

  /**
   * 打印程序运行的时间
   */
  private def printRunningTime(startTime: Long) {
    logInfo(Utils.logWrapper(s"程序共运行${(System.currentTimeMillis() - startTime) / 1000}秒"))
  }

  /**
   * 如果程序在运行过程中出现错误。那么在程序的最后打印出这些错误。
   * 之所以这么做是因为，Spark的Info日志太多，往往会把错误的日志淹没。
   */
  private def printErrorLogsIfExist() {
    if (Constants.App.ERROR_LOG.toString != "") {
      logError(Utils.logWrapper(s"程序在运行过程中遇到了如下错误：${Constants.App.ERROR_LOG.toString}"))
    }
  }

  /**
   * 最后打印出一些提示日志
   */
  private def printEndLogs(startTime: Long): Unit = {
    printErrorLogsIfExist()
    printRunningTime(startTime: Long)
  }
}