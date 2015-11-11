package com.asto.dmp.ycd.service.impl

import com.asto.dmp.ycd.base.Constants
import com.asto.dmp.ycd.dao.impl.BizDao
import com.asto.dmp.ycd.mq.{Msg, MQAgent}
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
    BizDao.monthsNumFromEarliestOrder.leftOuterJoin(BizDao.payMoneyAnnAvg)
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

  def sendIndexesToMQ() {
    val fields = FieldsCalculationService.getCalcFields.map(t => (t._2, t._3, t._4, t._5, t._6, t._7, t._8, t._9, t._10, t._11)).collect()(0)
    MQAgent.send(
      "indexes",
      Msg("M_MONTHS_NUM_FROM_CREATED", fields._1), //经营期限（月）
      Msg("Y_PAY_MONEY_ANN_AVG", fields._2), //订货额年均值
      Msg("Y_ORDER_AMOUNT_ANN_AVG", fields._3), //订货条数年均值
      Msg("Y_PER_CIGAR_AVG_PRICE_OF_ANN_AVG", fields._4), //每条均价年均值
      Msg("M_ACTIVE_CATEGORY_LAST_MONTH", fields._5), //当前月活跃品类
      Msg("Y_GROSS_MARGIN_LAST_YEAR", fields._6), //近1年毛利率
      Msg("M_MONTHLY_SALES_GROWTH_RATIO", fields._7), //月销售增长比
      Msg("M_SALES_RENT_RATIO", fields._8), //销售额租金比
      Msg("Y_CATEGORY_CONCENTRATION", fields._9), //品类集中度
      Msg("M_OFFLINE_SHOPPING_DISTRICT_INDEX", fields._10) //线下商圈指数
    )
  }
}

class FieldsCalculationService extends Service {
  override protected def runServices(): Unit = {
    if (Constants.App.MQ_ENABLE) {
      FieldsCalculationService.sendIndexesToMQ()
    }
    FileUtils.saveAsTextFile(BizDao.grossMarginPerMonthCategory, Constants.OutputPath.GROSS_MARGIN_PER_MONTH_CATEGORY)
    FileUtils.saveAsTextFile(BizDao.grossMarginPerMonthAll, Constants.OutputPath.GROSS_MARGIN_PER_MONTH_ALL)
    FileUtils.saveAsTextFile(BizDao.getActiveCategoryInLast12Months, Constants.OutputPath.ACTIVE_CATEGORY)
    FileUtils.saveAsTextFile(FieldsCalculationService.getCalcFields, Constants.OutputPath.FIELD)
  }
}
