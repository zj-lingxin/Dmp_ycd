package com.asto.dmp.ycd.service.impl

import com.asto.dmp.ycd.base.Constants
import com.asto.dmp.ycd.dao.impl.BizDao
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
    BizDao.monthsNumFromEarliestOrder.leftOuterJoin(BizDao.moneyAmountAnnAvg)
      .map(t => (t._1, (t._2._1, t._2._2.get)))
      .leftOuterJoin(BizDao.orderAmountAnnAvg)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._2.get))) // monthsNumFromEarliestOrder,payMoneyAnnAvg,orderAmountAnnAvg
      .leftOuterJoin(BizDao.perCigarAvgPriceOfAnnAvg)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._2.get))) //monthsNumFromEarliestOrder,payMoneyAnnAvg,orderAmountAnnAvg,perCigarAvgPriceOfAnnAvg
      .leftOuterJoin(BizDao.getActiveCategoryInLastMonth)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._2.get))) //
      .leftOuterJoin(BizDao.grossMarginLastYear)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._2.get)))
      .leftOuterJoin(BizDao.monthlySalesGrowthRatio)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._2.get)))
      .leftOuterJoin(BizDao.salesRentRatio)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._1._7, t._2._2.get)))
      .leftOuterJoin(BizDao.categoryConcentration)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._1._7, t._2._1._8, t._2._2.get)))
      .leftOuterJoin(BizDao.offlineShoppingDistrictIndex)
      .map(t => (t._1, t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._1._7, t._2._1._8, t._2._1._9, t._2._2.get))
  }

  /**
   * 向MQ发送各项指标
   */
  private def sendIndexes() {
    val fields = FieldsCalculationService.getCalcFields.map(t => (t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11)).collect()(0)
    BizUtils.handleMessage(
      MsgWrapper.getJson("各项指标",
        Msg("M_MONTHS_NUM_FROM_CREATED", fields._1), //经营期限（月）
        Msg("Y_PAY_MONEY_ANN_AVG", fields._2), //订货额年均值
        Msg("Y_ORDER_AMOUNT_ANN_AVG", fields._3), //订货条数年均值
        Msg("Y_PER_CIGAR_AVG_PRICE_OF_ANN_AVG", fields._4), //每条均价年均值
        Msg("M_ACTIVE_CATEGORY_LAST_MONTH", fields._5), //当前月活跃品类
        Msg("Y_GROSS_MARGIN_LAST_YEAR", fields._6), //近1年毛利率
        Msg("M_MONTHLY_SALES_GROWTH_RATIO", fields._7), //月销售增长比
        Msg("M_SALES_RENT_RATIO", fields._8), //销售额租金比
        Msg("Y_CATEGORY_CONCENTRATION", fields._9), //品类集中度
        Msg("M_OFFLINE_SHOPPING_DISTRICT_INDEX", fields._10)) //线下商圈指数)
    )
  }

  /**
   * 向MQ发送月订货额
   */
  private def sendMoneyAmount() = {
/*    BizUtils.handleMessage(
      MsgWrapper.getJson(
        "订货额",
        for(elem <- BizDao.moneyAmountPerMonth.collect().toList) yield Msg("M_PAY_MONEY", elem ._2, "2", elem ._1)
      )
    )*/
  }

  /**
   * 向MQ发送月订货条数
   */
  private def sendOrderAmount() = {
    val allOrderAmountPerMonth = BizDao.orderAmountPerMonth.collect().toList.map(t => (t._1._1,(t._1._2,t._2))).groupBy(t => t._1)
    allOrderAmountPerMonth.foreach { orderAmountPerMonth =>
      BizUtils.handleMessage(
        MsgWrapper.getJson("订货条数", for(elem <- orderAmountPerMonth._2) yield Msg("M_ORDER_BRANCHES_AMOUNT", elem._2._2, "2", elem._2._1), orderAmountPerMonth._1)
      )
    }
/*    BizUtils.handleMessage(
      MsgWrapper.getJson("订货条数", for(elem <- BizDao.orderAmountPerMonth.collect().toList) yield Msg("M_ORDER_BRANCHES_AMOUNT", elem._2, "2", elem._1._2))
    )*/
  }

  /**
   * 发送月订货品类数
   */
  private def sendCategory() = {
    BizUtils.handleMessage(
      MsgWrapper.getJson("订货品类数", for(elem <- BizDao.categoryPerMonth) yield Msg("M_CATEGORY_AMOUNT", elem._2, "2", elem._1))
    )
  }

  /**
   * 发送月订货次数
   * priv
   */
  private def sendOrderNumber() = {
    BizUtils.handleMessage(
      MsgWrapper.getJson("订货次数", for(elem <- BizDao.orderNumberPerMonth) yield Msg("M_ORDER_TIMES_AMOUNT", elem._2, "2", elem._1))
    )
  }

  /**
   * 发送每条均价
   */
  private def sendPerCigarPrice() = {
 /*   BizUtils.handleMessage(
      MsgWrapper.getJson("每条均价", for(elem <- BizDao.perCigarPricePerMonth) yield Msg("M_PER_CIGAR_PRICE", elem._2, "2", elem._1))
    )*/
  }

  private def sendActiveCategory() = {
    BizUtils.handleMessage(
      MsgWrapper.getJson(
        "活跃品类",
        {for (elem <- BizDao.getActiveCategoryFor(Constants.App.STORE_ID, (1, 12))) yield Msg("M_ACTIVE_CATEGORY", elem._3, "2", elem._2)}.toList
      )
    )
  }

  private def sendMoneyAmountTop5PerMonth() = {
    val moneyAmountTop5PerMonth = BizDao.payMoneyTop5PerMonth
    (0 to 4).foreach{ i =>
      BizUtils.handleMessage(
        MsgWrapper.getJson(
          s"近12个月订货额top${i+1}",
          {for(elem <- moneyAmountTop5PerMonth) yield MsgWithName(s"M_PAY_MONEY_TOP${i+1}",  elem._2(i)._2, elem._2(i)._1, "2", elem._1)}.toList
        )
      )
    }
  }

  private def sendMessageToMQ() = {
   sendMoneyAmount()
    /*   sendIndexes()*/
    sendOrderAmount()
/*    sendCategory()
    sendOrderNumber()
    sendPerCigarPrice()
    sendActiveCategory()
    sendMoneyAmountTop5PerMonth()*/
  }
}

class FieldsCalculationService extends Service {
  override protected def runServices(): Unit = {
    FieldsCalculationService.sendMessageToMQ()
    /*
     FileUtils.saveAsTextFile(BizDao.grossMarginPerMonthCategory, Constants.OutputPath.GROSS_MARGIN_PER_MONTH_CATEGORY)
     FileUtils.saveAsTextFile(BizDao.grossMarginPerMonthAll, Constants.OutputPath.GROSS_MARGIN_PER_MONTH_ALL)
     FileUtils.saveAsTextFile(BizDao.getActiveCategoryInLast12Months, Constants.OutputPath.ACTIVE_CATEGORY)
     FileUtils.saveAsTextFile(FieldsCalculationService.getCalcFields, Constants.OutputPath.FIELD)
     */
  }
}
