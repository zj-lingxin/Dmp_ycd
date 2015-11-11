package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base.{Service, Constants}
import com.asto.dmp.ycd.dao.BizDao
import com.asto.dmp.ycd.service.ScoreService._
import com.asto.dmp.ycd.util.{FileUtils, Utils}

object ScoreService {

  /** 权重 **/
  object Weight {
    //规模	权重:30%	 订货额年均值	近1年月均（提货额）	0≤(X-50000)/100000≤1
    val payMoneyAnnAvg = 0.3

    //规模	权重:5% 订货条数年均值 平均每月订货量（条）
    val orderAmountAnnAvg = 0.05

    //盈利	权重:10%	 销售额租金比	月均提货额 除 租赁合同月均租金额  	0≤X/10≤1
    val salesRentRatio = 0.1

    //盈利 权重:10%	 毛利率	1年毛利率	 0≤(X-14%)/3%≤1
    val grossMarginLastYear = 0.1

    //成长	权重:20%	月销售增长比	近3月平均销售/近6月平均销售
    val monthlySalesGrowthRatio = 0.2

    //运营 权重:5%	每条均价年均值	客单价（每条进货均价）0≤(X-120)/120≤1
    val perCigarAvgPriceOfAnnAvg = 0.05

    //运营	权重:5%	经营期限（月）申报月起经营月份数 减 最早一笔网上订单的月份
    val monthsNumsFromEarliestOrder = 0.05

    //运营	权重:5%	活跃品类最近一个月的值才参与模型计算	月均活跃品类 减 基准值20种
    val activeCategoryInLastMonth = 0.05

    //运营	权重:5%	品类集中度	金额TOP10的金额占比
    val categoryConcentration = 0.05

    //市场	权重:5%	线下商圈指数	商圈指数100%分制，默认值为80%
    val offlineShoppingDistrictIndex = 0.05
  }

  private def finalScore(gpa: Double) = {
    Utils.retainDecimal(gpa * 150 + 500, 0).toInt
  }

  /** 获取规模得分 **/
  private def getScaleScore(payMoneyAnnAvgGPA: Double, orderAmountAnnAvgGPA: Double) =
    finalScore((Weight.payMoneyAnnAvg * payMoneyAnnAvgGPA + Weight.orderAmountAnnAvg * orderAmountAnnAvgGPA) / (Weight.payMoneyAnnAvg + Weight.orderAmountAnnAvg))

  /** 获取盈利得分 **/
  private def getProfitScore(salesRentRatioGPA: Double, grossMarginLastYearGPA: Double) =
    finalScore((Weight.salesRentRatio * salesRentRatioGPA + Weight.grossMarginLastYear * grossMarginLastYearGPA) / (Weight.salesRentRatio + Weight.grossMarginLastYear))

  /** 获取成长得分 **/
  private def getGrowingUpScore(monthlySalesGrowthRatioGPA: Double) = finalScore(monthlySalesGrowthRatioGPA)

  /** 获取运营得分 **/
  private def getOperationScore(perCigarAvgPriceOfAnnAvgGPA: Double, monthsNumFromEarliestOrderGPA: Double, activeCategoryInLastMonthGPA: Double, categoryConcentrationGPA: Double) =
    finalScore((Weight.perCigarAvgPriceOfAnnAvg * perCigarAvgPriceOfAnnAvgGPA + Weight.monthsNumsFromEarliestOrder * monthsNumFromEarliestOrderGPA + Weight.activeCategoryInLastMonth * activeCategoryInLastMonthGPA + Weight.categoryConcentration * categoryConcentrationGPA) / (Weight.perCigarAvgPriceOfAnnAvg + Weight.monthsNumsFromEarliestOrder + Weight.activeCategoryInLastMonth + Weight.categoryConcentration))

  /** 获取市场得分 **/
  private def getMarketScore(offlineShoppingDistrictIndexGPA: Double) = finalScore(offlineShoppingDistrictIndexGPA)

  /**
   * 总评分
   * @param allGPA Tuple10(scoreOfPayMoneyAnnAvgGPA, orderAmountAnnAvgGPA, salesRentRatioGPA, grossMarginLastYearGPA, monthlySalesGrowthRatioGPA, scoreOfPerCigarAvgPriceOfAnnAvgGPA, monthsNumFromEarliestOrderGPA, activeCategoryInLastMonthGPA, categoryConcentrationGPA, offlineShoppingDistrictIndexGPA)
   */
  private def getTotalScore(allGPA: Tuple10[Double, Double, Double, Double, Double, Double, Double, Double, Double, Double]) = {
    finalScore(Weight.payMoneyAnnAvg * allGPA._1 + Weight.orderAmountAnnAvg * allGPA._2 + Weight.salesRentRatio * allGPA._3 + Weight.grossMarginLastYear * allGPA._4 + Weight.monthlySalesGrowthRatio * allGPA._5 + Weight.perCigarAvgPriceOfAnnAvg * allGPA._6 + Weight.monthsNumsFromEarliestOrder * allGPA._7 + Weight.activeCategoryInLastMonth * allGPA._8 + Weight.categoryConcentration * allGPA._9 + Weight.offlineShoppingDistrictIndex * allGPA._10)
  }

  private def calcPayMoneyAnnAvgGPA(value: Double) = rangeOfGPA((value - 50000) / 100000)

  private def calcOrderAmountAnnAvgGPA(value: Double) = rangeOfGPA((value - 300) / 300)

  private def calcSalesRentRatioGPA(value: Double) = rangeOfGPA(value / 10)

  private def calcGrossMarginLastYearGPA(value: Double) = rangeOfGPA((value - 0.14) / 0.03)

  private def calcMonthlySalesGrowthRatioGPA(value: Double) = rangeOfGPA(value / 1.25)

  private def calcPerCigarAvgPriceOfAnnAvgGPA(value: Double) = rangeOfGPA((value - 120) / 120)

  private def calcMonthsNumFromEarliestOrderGPA(value: Double) = rangeOfGPA((value - 12) / 6)

  private def calcActiveCategoryInLastMonthGPA(value: Double) = rangeOfGPA((value - 20) / 20)

  private def calcCategoryConcentrationGPA(value: Double) = rangeOfGPA((value - 0.7) / 0.3)

  private def calcOfflineShoppingDistrictIndexGPA(value: Double) = rangeOfGPA(value)

  /**
   * 获取规模相关的绩点
   * 返回：(licenseNo,(payMoneyAnnAvgGPA, orderAmountAnnAvgGPA))
   * 中文：(店铺id  ,(   订货额年均值绩点,      订货条数年均值绩点))
   */
  private def getScaleGPA = {
    val payMoneyAnnAvgGPA = BizDao.payMoneyAnnAvg
      .map(t => (t._1, calcPayMoneyAnnAvgGPA(t._2)))

    val orderAmountAnnAvgGPA = BizDao.orderAmountAnnAvg
      .map(t => (t._1, calcOrderAmountAnnAvgGPA(t._2)))
    payMoneyAnnAvgGPA.leftOuterJoin(orderAmountAnnAvgGPA).map(t => (t._1, (t._2._1, t._2._2.get)))
  }

  /**
   * 获取盈利相关的绩点
   * 返回：(licenseNo,(salesRentRatioGPA,grossMarginLastYearGPA))
   * 中文：(店铺id  ,(   销售额租金比绩点,           1年毛利率绩点))
   */
  private def getProfitGPA = {
    val salesRentRatioGPA = BizDao.salesRentRatio
      .map(t => (t._1, calcSalesRentRatioGPA(t._2)))
    val grossMarginLastYearGPA = BizDao.grossMarginLastYear
      .map(t => (t._1, calcGrossMarginLastYearGPA(t._2)))
    salesRentRatioGPA.leftOuterJoin(grossMarginLastYearGPA).map(t => (t._1, (t._2._1, t._2._2.get)))
  }

  /**
   * 获取成长相关的绩点
   * 返回：(licenseNo, monthlySalesGrowthRatioGPA)
   * 中文：(店铺id  ,             月销售增长比绩点)
   */
  private def getGrowingUpGPA = {
    val monthlySalesGrowthRatioGPA = BizDao.monthlySalesGrowthRatio
      .map(t => (t._1, calcMonthlySalesGrowthRatioGPA(t._2)))
    monthlySalesGrowthRatioGPA
  }

  /**
   * 获取运营相关的绩点
   * 返回：(licenseNo, (perCigarAvgPriceOfAnnAvgGPA, monthsNumsFromEarliestOrderGPA,activeCategoryInLastMonthGPA,categoryConcentrationGPA))
   * 中文：(店铺id  , (           每条均价年均值绩点 ,                    经营期限绩点,                  活跃品类绩点,            品类集中度绩点))
   */
  private def getOperationGPA = {
    val perCigarAvgPriceOfAnnAvgGPA = BizDao.perCigarAvgPriceOfAnnAvg
      .map(t => (t._1, calcPerCigarAvgPriceOfAnnAvgGPA(t._2)))
    val monthsNumFromEarliestOrderGPA = BizDao.monthsNumFromEarliestOrder
      .map(t => (t._1, calcMonthsNumFromEarliestOrderGPA(t._2)))
    val activeCategoryInLastMonthGPA = BizDao.getActiveCategoryInLastMonth
      .map(t => (t._1, calcActiveCategoryInLastMonthGPA(t._2)))
    val categoryConcentrationGPA = BizDao.categoryConcentration
      .map(t => (t._1, calcCategoryConcentrationGPA(t._2)))
    val tempOperationsGPA1 = perCigarAvgPriceOfAnnAvgGPA.leftOuterJoin(monthsNumFromEarliestOrderGPA).map(t => (t._1, (t._2._1, t._2._2.get)))
    val tempOperationsGPA2 = activeCategoryInLastMonthGPA.leftOuterJoin(categoryConcentrationGPA).map(t => (t._1, (t._2._1, t._2._2.get)))
    tempOperationsGPA1.leftOuterJoin(tempOperationsGPA2).map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._2.get._1, t._2._2.get._2)))
  }

  /**
   * 获取市场相关的绩点
   * 返回：(licenseNo, offlineShoppingDistrictIndexGPA)
   * 中文：(店铺id  ,                      线下商圈指数)
   */
  private def getMarketGPA = {
    val offlineShoppingDistrictIndexGPA = BizDao.offlineShoppingDistrictIndex
      .map(t => (t._1, calcOfflineShoppingDistrictIndexGPA(t._2)))
    offlineShoppingDistrictIndexGPA
  }

  /**
   * 获取所有的绩点
   * 返回：(licenseNo,(payMoneyAnnAvgGPA,perCigarAvgPriceOfAnnAvgGPA,salesRentRatioGPA,grossMarginLastYearGPA,
   * monthlySalesGrowthRatioGPA,orderAmountAnnAvgGPA,monthsNumsFromEarliestOrderGPA,
   * activeCategoryInLastMonthGPA,categoryConcentrationGPA,offlineShoppingDistrictIndexGPA))
   * 中文：(店铺id  ,( 订货额年均值绩点,每条均价年均值绩点,销售额租金比绩点,1年毛利率绩点,月销售增长比绩点,订货条数年均值绩点,经营期限绩点,活跃品类绩点,品类集中度绩点,线下商圈指数))
   */
  private def getAllGPA = {
    getScaleGPA.leftOuterJoin(getProfitGPA) //(33010120120716288A,((0.56734,0.49166666666666664),Some((0.0,0.5999999999999996))))
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._2.get._1, t._2._2.get._2))) //(33010120120716288A,(0.56734,0.49166666666666664,0.0,0.5999999999999996))
      .leftOuterJoin(getGrowingUpGPA)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._2.get))) //(33010120120716288A,(0.56734,0.49166666666666664,0.0,0.5999999999999996,0.76))
      .leftOuterJoin(getOperationGPA)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._2.get._1, t._2._2.get._2, t._2._2.get._3, t._2._2.get._4))) //(33010120120716288A,(0.56734,0.49166666666666664,0.0,0.5999999999999996,0.76,0.9766666666666667,1.0,0.5,0.43333333333333335))
      .leftOuterJoin(getMarketGPA)
      .map(t => (t._1, (t._2._1._1, t._2._1._2, t._2._1._3, t._2._1._4, t._2._1._5, t._2._1._6, t._2._1._7, t._2._1._8, t._2._1._9, t._2._2.get))).persist() //(33010120120716288A,(0.56734,0.49166666666666664,0.0,0.5999999999999996,0.76,0.9766666666666667,1.0,0.5,0.43333333333333335,0.8))
  }

  /**
   * getAllGPA这个RDD调整后的格式，用于输出到文件的格式
   * 输出：店铺id，订货额年均值绩点，每条均价年均值绩点，	销售额租金比绩点，1年毛利率绩点，月销售增长比绩点，订货条数年均值绩点，	经营期限绩点，活跃品类绩点，品类集中度绩点，线下商圈指数
   */
  def getResultGPA = getAllGPA.map(t => (t._1, t._2._1, t._2._2, t._2._3, t._2._4, t._2._5, t._2._6, t._2._7, t._2._8, t._2._9, t._2._10))

  /**
   * 输出：店铺id，规模得分	，盈利得分，成长得分，运营得分	，市场得分，总得分
   */
  def getAllScore = {
    getAllGPA.map(t => (t._1, getScaleScore(t._2._1, t._2._2), getProfitScore(t._2._3, t._2._4), getGrowingUpScore(t._2._5), getOperationScore(t._2._6, t._2._7, t._2._8, t._2._9), getMarketScore(t._2._10), getTotalScore(t._2))).cache()
  }

  private def rangeOfGPA(GPA: Double) = {
    if (GPA > 1) 1D
    else if (GPA < 0) 0D
    else GPA
  }
}

/**
 * 评分规则
 */
class ScoreService extends Service {
  override def runServices() =  {
    FileUtils.saveAsTextFile(getResultGPA, Constants.OutputPath.GPA)
    FileUtils.saveAsTextFile(getAllScore, Constants.OutputPath.SCORE)
  }
}


