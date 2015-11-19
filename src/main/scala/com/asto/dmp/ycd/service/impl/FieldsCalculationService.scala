package com.asto.dmp.ycd.service.impl

import com.asto.dmp.ycd.base.Constants
import com.asto.dmp.ycd.dao.impl.BizDao
import com.asto.dmp.ycd.dao.impl.BizDao._
import com.asto.dmp.ycd.mq._
import com.asto.dmp.ycd.service.Service
import com.asto.dmp.ycd.util.FileUtils

object FieldsCalculationService {
  /**
   * 返回计算绩点和得分所需的字段
   * 返回：(licenseNo,monthsNumFromEarliestOrder,payMoneyAnnAvg,orderAmountAnnAvg,perCigarAvgPriceOfAnnAvg,
   * activeCategoryInLastMonth,grossMarginLastYear,monthlySalesGrowthRatio,salesRentRatio,
   * categoryConcentration,offlineShoppingDistrictIndex)
   * 中文：(店铺id,经营期限（月）,订货额年均值,订货条数年均值,每条均价年均值,当前月活跃品类,近1年毛利率,月销售增长比,销售额租金比,品类集中度,线下商圈指数)
   */
  def getCalcFields = {
    monthsNumFromEarliestOrder.leftOuterJoin(moneyAmountAnnAvg)
      .map(t => (t._1, (t._2._1, t._2._2.get)))
      .leftOuterJoin(orderAmountAnnAvg)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._2.get))) // monthsNumFromEarliestOrder,payMoneyAnnAvg,orderAmountAnnAvg
      .leftOuterJoin(perCigarAvgPriceOfAnnAvg)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._2.get))) //monthsNumFromEarliestOrder,payMoneyAnnAvg,orderAmountAnnAvg,perCigarAvgPriceOfAnnAvg
      .leftOuterJoin(getActiveCategoryInLastMonth)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._2.get))) //
      .leftOuterJoin(grossMarginLastYear)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._2.get)))
      .leftOuterJoin(monthlySalesGrowthRatio)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._2.get)))
      .leftOuterJoin(salesRentRatio)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._1._7, t._2._2.get)))
      .leftOuterJoin(categoryConcentration)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._1._7, t._2._1._8, t._2._2.get)))
      .leftOuterJoin(offlineShoppingDistrictIndex)
      .map(t => (t._1, t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._1._7, t._2._1._8, t._2._1._9, t._2._2.get))
  }


  /**
   * 向MQ发送各项指标
   */
  private def sendIndexes() {
    val strMsgsOfAllStores = new StringBuffer()
    //FieldsCalculationService.getCalcFields.foreach(println)
    FieldsCalculationService.getCalcFields.collect().foreach {
      eachStore =>
        val msgs = List[Msg](
          Msg("M_MONTHS_NUM_FROM_CREATED", eachStore._2), //经营期限（月）
          Msg("Y_PAY_MONEY_ANN_AVG", eachStore._3), //订货额年均值
          Msg("Y_ORDER_AMOUNT_ANN_AVG", eachStore._4), //订货条数年均值
          Msg("Y_PER_CIGAR_AVG_PRICE_OF_ANN_AVG", eachStore._5), //每条均价年均值
          Msg("M_ACTIVE_CATEGORY_LAST_MONTH", eachStore._6), //当前月活跃品类
          Msg("Y_GROSS_MARGIN_LAST_YEAR", eachStore._7), //近1年毛利率
          Msg("M_MONTHLY_SALES_GROWTH_RATIO", eachStore._8), //月销售增长比
          Msg("M_SALES_RENT_RATIO", eachStore._9), //销售额租金比
          Msg("Y_CATEGORY_CONCENTRATION", eachStore._10), //品类集中度
          Msg("M_OFFLINE_SHOPPING_DISTRICT_INDEX", eachStore._11) //线下商圈指数)
        )
        MQAgent.send(MsgWrapper.getJson("各项指标", msgs, eachStore._1))
        strMsgsOfAllStores.append(Msg.strMsgsOfAStore("各项指标", eachStore._1, msgs))
    }
    FileUtils.saveAsTextFile(strMsgsOfAllStores.toString, Constants.OutputPath.INDEXES_PATH)
  }

  /**
   * 向MQ发送月订货额
   */
  private def sendMoneyAmount() = {
    commonStepsForSaveData(
      moneyAmountPerMonth.map(t => (t._1._1, (t._1._2, t._2))).groupByKey().collect(),
      "M_PAY_MONEY",
      "订货额",
      Constants.OutputPath.MONEY_AMOUNT_PATH
    )
  }


  /**
   * 向MQ发送月订货条数
   */
  private def sendOrderAmount() = {
    commonStepsForSaveData(
      orderAmountPerMonth.map(t => (t._1._1, (t._1._2, t._2))).groupByKey().collect(),
      "M_ORDER_BRANCHES_AMOUNT",
      "订货条数",
      Constants.OutputPath.ORDER_AMOUNT_PATH
    )
  }

  private def commonStepsForSaveData[T](storesInfoArray: Array[(String, Iterable[(String, T)])], quotaCode: String, quotaItemName: String, outputPath: String) = {
    val strMsgsOfAllStores = new StringBuffer()
    storesInfoArray.foreach {
      eachStore =>
        val msgs = for (elem <- eachStore._2.toList) yield Msg(quotaCode, elem._2, "2", elem._1)
        MQAgent.send(MsgWrapper.getJson(quotaItemName, msgs, eachStore._1))
        strMsgsOfAllStores.append(Msg.strMsgsOfAStore(quotaItemName, eachStore._1, msgs))
    }
    FileUtils.saveAsTextFile(strMsgsOfAllStores.toString, outputPath)
  }

  /**
   * 发送月订货品类数
   */
  private def sendCategoryAmount() = {
    commonStepsForSaveData(
      categoryPerMonth.groupByKey().collect(),
      "M_CATEGORY_AMOUNT",
      "订货品类数",
      Constants.OutputPath.CATEGORY_AMOUNT_PATH
    )
  }

  /**
   * 发送月订货次数
   * priv
   */
  private def sendOrderNumber() = {
    commonStepsForSaveData(
      orderNumberPerMonth.groupByKey().collect(),
      "M_ORDER_TIMES_AMOUNT",
      "订货次数",
      Constants.OutputPath.ORDER_NUMBER_PATH
    )
  }

  /**
   * 发送每条均价
   */
  private def sendPerCigarPrice() = {
    commonStepsForSaveData(
      perCigarPricePerMonth.groupByKey().collect(),
      "M_PER_CIGAR_PRICE",
      "每条均价",
      Constants.OutputPath.PER_CIGAR_PRICE_PATH
    )
  }

  private def sendActiveCategory() = {
    val strMsgsOfAllStores = new StringBuffer()
    BizDao.getNewActiveCategoryInLast12Months(1, 12).groupBy(_._1).foreach {
      eachStore =>
        val msgs = for (elem <- eachStore._2.toList) yield Msg("M_ACTIVE_CATEGORY", elem._3, "2", elem._2)
        MQAgent.send(MsgWrapper.getJson("活跃品类", msgs, eachStore._1))
        strMsgsOfAllStores.append(Msg.strMsgsOfAStore("活跃品类", eachStore._1, msgs))
    }
    FileUtils.saveAsTextFile(strMsgsOfAllStores.toString, Constants.OutputPath.ACTIVE_CATEGORY_PATH)
  }

  private def sendMoneyAmountTop5PerMonth() = {
    val strMsgsOfAllStores = new StringBuffer()
    BizDao.payMoneyTop5PerMonth.foreach {
      eachStore =>
        val lastYearData = eachStore._2.toList
        (0 to 4).foreach { i =>
          val msgs = for (eachMonthData <- lastYearData) yield setMsgWithNameFor(eachMonthData, i)
          MQAgent.send(MsgWrapper.getJson(s"近12个月订货额top${i + 1}", msgs, eachStore._1))
          strMsgsOfAllStores.append(MsgWithName.strMsgsOfAStore(s"近12个月订货额top${i + 1}", eachStore._1, msgs))
        }
    }
    FileUtils.saveAsTextFile(strMsgsOfAllStores.toString, Constants.OutputPath.MONEY_AMOUNT_TOP5_PATH)
  }

  private def setMsgWithNameFor(eachMonthData: (String, List[(Double, String)]), topIndex: Int) = {
    if (eachMonthData._2.length > topIndex)
      MsgWithName(s"M_PAY_MONEY_TOP${topIndex + 1}", eachMonthData._2(topIndex)._2, eachMonthData._2(topIndex)._1, "2", eachMonthData._1)
    else
      MsgWithName(s"M_PAY_MONEY_TOP${topIndex + 1}", "Null", 0, "2", eachMonthData._1)
  }

  private def sendMessageToMQ() = {
    sendMoneyAmount()
    sendOrderAmount()
    sendCategoryAmount()
    sendOrderNumber()
    sendPerCigarPrice()
    sendActiveCategory()
    sendMoneyAmountTop5PerMonth()
    sendIndexes()
  }
}

class FieldsCalculationService extends Service {
  override protected def runServices(): Unit = {
    FieldsCalculationService.sendMessageToMQ()
  }
}
