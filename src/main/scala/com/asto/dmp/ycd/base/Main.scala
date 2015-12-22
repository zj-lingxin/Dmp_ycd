package com.asto.dmp.ycd.base

import com.asto.dmp.ycd.dao.impl.{BizDao, BaseDao}
import com.asto.dmp.ycd.mq.MQAgent
import com.asto.dmp.ycd.service.impl.{LoanWarnService, FieldsCalculationService, CreditService, ScoreService}
import com.asto.dmp.ycd.util.{DateUtils, Utils}
import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    if (argsIsIllegal(args)) return
    runServicesBy(args)
    closeResources()
    printEndLogs(startTime)
  }

  private def runServicesBy(args: Array[String]) {
    Constants.App.TIMESTAMP = args(1).toLong
    //从外部传入的是秒级别的时间戳，所以要乘以1000
    Constants.App.TODAY = DateUtils.timestampToStr(Constants.App.TIMESTAMP * 1000, "yyyyMM/dd")
    Constants.App.RUN_CODE = args(0)
    args(0) match {
      case "100" =>
        Constants.App.STORE_ID = args(2)
        Constants.App.IS_ONLINE = true
        logInfo(Utils.logWrapper(s"运行[在线模型-计算单个店铺],店铺ID为：${Constants.App.STORE_ID}"))
        new FieldsCalculationService().run()
        new ScoreService().run()
        new CreditService().run()
      case "200" =>
        Constants.App.IS_ONLINE = false
        logInfo(Utils.logWrapper("运行[离线模型-计算所有店铺]"))

        BizDao.moneyAmountPerMonth.foreach(println)
        BizDao.moneyAmountPerMonthNew.foreach(println)
     /*   new FieldsCalculationService().run()
        new ScoreService().run()*/
        //授信额度这里不需要计算
      case "201" =>
        Constants.App.IS_ONLINE = false
        logInfo(Utils.logWrapper(s"运行[离线模型-计算贷后预警]"))
        new LoanWarnService().run()
      case _ =>
        logInfo(
          Utils.logWrapper(
            s"可选模型参数如下:\n" +
              s"[在线模型-计算单个店铺]:100,时间戳,店铺Id\n" +
              s"[离线模型-计算所有店铺]:200,时间戳\n" +
              s"[离线模型-计算贷后预警]:201,时间戳\n"
          )
        )
    }
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
      logError(Utils.logWrapper("请传入程序参数:业务编号,时间戳,[店铺Id]"))
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