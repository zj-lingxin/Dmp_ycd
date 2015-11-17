package com.asto.dmp.ycd.service.impl

import com.asto.dmp.ycd.dao.impl.BizDao
import com.asto.dmp.ycd.dao.impl.BizDao._
import com.asto.dmp.ycd.mq._
import com.asto.dmp.ycd.service.Service
import com.asto.dmp.ycd.util.BizUtils

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
    FieldsCalculationService.getCalcFields.foreach {
      eachStore =>
        BizUtils.handleMessage(
          MsgWrapper.getJson(
            "各项指标",
            List(
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
            ),
            eachStore._1
          )
        )
    }
  }

  /**
   * 向MQ发送月订货额
   */
  private def sendMoneyAmount() = {
    moneyAmountPerMonth.map(t => (t._1._1, (t._1._2, t._2))).groupByKey().collect().foreach {
      eachStore =>
        BizUtils.handleMessage(
          MsgWrapper.getJson(
            "订货额",
            for (elem <- eachStore._2.toList) yield Msg("M_PAY_MONEY", elem._2, "2", elem._1),
            eachStore._1
          )
        )
    }
  }

  /**
   * 向MQ发送月订货条数
   */
  private def sendOrderAmount() = {
    orderAmountPerMonth.map(t => (t._1._1, (t._1._2, t._2))).groupByKey().collect().foreach {
      eachStore =>
        BizUtils.handleMessage(
          MsgWrapper.getJson(
            "订货条数",
            for (elem <- eachStore._2.toList) yield Msg("M_ORDER_BRANCHES_AMOUNT", elem._2, "2", elem._1),
            eachStore._1
          )
        )
    }
  }

  /**
   * 发送月订货品类数
   */
  private def sendCategory() = {
    categoryPerMonth.groupByKey().collect().foreach {
      categoryPerMonth =>
        BizUtils.handleMessage(
          MsgWrapper.getJson(
            "订货品类数",
            for (elem <- categoryPerMonth._2.toList) yield Msg("M_CATEGORY_AMOUNT", elem._2, "2", elem._1),
            categoryPerMonth._1
          )
        )
    }
  }

  /**
   * 发送月订货次数
   * priv
   */
  private def sendOrderNumber() = {
    orderNumberPerMonth.groupByKey().collect().foreach {
      eachStore =>
        BizUtils.handleMessage(
          MsgWrapper.getJson(
            "订货次数",
            for (elem <- eachStore._2.toList) yield Msg("M_ORDER_TIMES_AMOUNT", elem._2, "2", elem._1),
            eachStore._1
          )
        )
    }
  }

  /**
   * 发送每条均价
   */
  private def sendPerCigarPrice() = {
    perCigarPricePerMonth.groupByKey().collect().foreach {
      eachStore =>
        BizUtils.handleMessage(
          MsgWrapper.getJson(
            "每条均价",
            for (elem <- eachStore._2.toList) yield Msg("M_PER_CIGAR_PRICE", elem._2, "2", elem._1),
            eachStore._1
          )
        )
    }
  }

  private def sendActiveCategory() = {
    BizDao.getNewActiveCategoryInLast12Months(1, 12).groupBy(_._1).foreach {
      eachStore =>
        BizUtils.handleMessage(
          MsgWrapper.getJson(
            "活跃品类",
            for (elem <- eachStore._2.toList) yield Msg("M_ACTIVE_CATEGORY", elem._3, "2", elem._2),
            eachStore._1
          )
        )
    }
  }

  private def sendMoneyAmountTop5PerMonth() = {
    BizDao.payMoneyTop5PerMonth.foreach {
      eachStore =>
        val lastYearData = eachStore._2.toList
        (0 to 4).foreach { i =>
          BizUtils.handleMessage(
            MsgWrapper.getJson(
              s"近12个月订货额top${i + 1}",
              for (eachMonthData <- lastYearData) yield setMsgWithNameFor(eachMonthData, i),
              eachStore._1
            )
          )
        }
    }
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
    sendCategory()
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
