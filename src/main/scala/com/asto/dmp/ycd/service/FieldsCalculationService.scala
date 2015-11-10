package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base.{Service, Constants}
import com.asto.dmp.ycd.dao.{ServiceDao, BizDao}
import com.asto.dmp.ycd.util.FileUtils

object FieldsCalculationService {
  /**
   * 返回计算绩点和得分所需的字段
   * 返回：(licenseNo,monthsNumFromEarliestOrder,payMoneyAnnAvg,orderAmountAnnAvg,perCigarAvgPriceOfAnnAvg,
   *                  activeCategoryInLastMonth,grossMarginLastYear,monthlySalesGrowthRatio,salesRentRatio,
   *                  categoryConcentration,offlineShoppingDistrictIndex)
   * 中文：(店铺id,经营期限（月）,订货额年均值,订货条数年均值,每条均价年均值,当前月活跃品类,近1年毛利率,月销售增长比,销售额租金比,品类集中度,线下商圈指数)
   */
  def getCalcFields = {
    ServiceDao.monthsNumFromEarliestOrder.leftOuterJoin(ServiceDao.payMoneyAnnAvg)
      .map(t => (t._1, (t._2._1, t._2._2.get)))
      .leftOuterJoin(ServiceDao.orderAmountAnnAvg)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._2.get))) // monthsNumFromEarliestOrder,payMoneyAnnAvg,orderAmountAnnAvg
      .leftOuterJoin(ServiceDao.perCigarAvgPriceOfAnnAvg)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._2.get))) //monthsNumFromEarliestOrder,payMoneyAnnAvg,orderAmountAnnAvg,perCigarAvgPriceOfAnnAvg
      .leftOuterJoin(ServiceDao.getActiveCategoryInLastMonth)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._2.get))) //
      .leftOuterJoin(ServiceDao.grossMarginLastYear)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._2.get)))
      .leftOuterJoin(ServiceDao.monthlySalesGrowthRatio)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._2.get)))
      .leftOuterJoin(ServiceDao.salesRentRatio)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._1._7, t._2._2.get)))
      .leftOuterJoin(ServiceDao.categoryConcentration)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._1._7, t._2._1._8, t._2._2.get)))
      .leftOuterJoin(ServiceDao.offlineShoppingDistrictIndex)
      .map(t => (t._1, t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._1._7, t._2._1._8, t._2._1._9, t._2._2.get))
  }
}

class FieldsCalculationService extends Service {
  override protected def runServices(): Unit = {
    FileUtils.saveAsTextFile(ServiceDao.grossMarginPerMonthCategory, Constants.OutputPath.GROSS_MARGIN_PER_MONTH_CATEGORY)
    FileUtils.saveAsTextFile(ServiceDao.grossMarginPerMonthAll, Constants.OutputPath.GROSS_MARGIN_PER_MONTH_ALL)
    FileUtils.saveAsTextFile(ServiceDao.getActiveCategoryInLast12Months, Constants.OutputPath.ACTIVE_CATEGORY)
    FileUtils.saveAsTextFile(FieldsCalculationService.getCalcFields, Constants.OutputPath.FIELD)
  }

}
