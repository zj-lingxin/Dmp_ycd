package com.asto.dmp.ycd.base

import com.asto.dmp.ycd.dao.ScoreDao
import com.asto.dmp.ycd.service._
import com.asto.dmp.ycd.util.Utils
import org.apache.spark.Logging

object Main extends Logging {
  def main(args: Array[String]) {
    val startTime = System.currentTimeMillis()
    if (Option(args).isEmpty || args.length == 0) {
      logError(Utils.wrapLog("请传入模型编号：1~5"))
      return
    }
    args(0) match {
      case "1" =>
        new DataPrepareService().run()
      case "2" =>
        //new ScoreService().run()
        ScoreDao.grossMarginLastYearGroupByLicenseNo.foreach(println)
      case "3" =>
        //授信模型
        new CreditService().run()
      case "4" =>
        //贷后模型
        new ScoreService().run()
      case "5" =>
        //所有模型一起运行
        logInfo(Utils.wrapLog("所有模型一起运行"))
        new DataPrepareService().run()

      case _ =>
        logError(s"传入参数错误!传入的是${args(0)},请传入1~5")
    }

    Contexts.stopSparkContext()
    val endTime = System.currentTimeMillis()
    logInfo(s"程序共运行${(endTime - startTime) / 1000}秒")
  }
}