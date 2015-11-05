package com.asto.dmp.ycd.service

import com.asto.dmp.ycd.base.{Constants, DataSource}
import com.asto.dmp.ycd.dao.ScoreDao
import com.asto.dmp.ycd.util.mail.MailAgent
import com.asto.dmp.ycd.util.{FileUtils, Utils}

object ScoreService {
  //规模	权重:30%	 订货额年均值	近1年月均（提货额）	0≤(X-50000)/100000≤1
  val weightsOfPayMoneyAnnAvg = 0.3

  //规模	权重:5%	每条均价年均值	客单价（每条进货均价）0≤(X-120)/120≤1
  val weightsOfperCigarAvgPriceOfAnnAvg = 0.05

  //盈利	权重:10%	 销售额租金比	月均提货额 除 租赁合同月均租金额  	0≤X/10≤1
  val weightsOfsalesRentRatio = 0.1

  //盈利 权重:10%	 毛利率	1年毛利率	 0≤(X-14%)/3%≤1
  val weightsOfgrossMarginLastYear = 0.1

  //成长	权重:20%	月销售增长比	近3月平均销售/近6月平均销售
  val weightsOfMonthlySalesGrowthRatio = 0.2

  //运营 权重:5%	订货条数年均值 平均每月订货量（条）
  val weightsOfOrderAmountAnnAvg = 0.05

  //运营	权重:5%	经营期限（月）申报月起经营月份数 减 最早一笔网上订单的月份
  val weightsOfMonthsNumsFromEarliestOrder = 0.05

  //运营	权重:5%	活跃品类最近一个月的值才参与模型计算	月均活跃品类 减 基准值20种
  val weightsOfActiveCategoryInLastMonth = 0.05

  //运营	权重:5%	品类集中度	金额TOP10的金额占比
  val weightsOfCategoryConcentration = 0.05

  //市场	权重:5%	线下商圈指数	商圈指数100%分制，默认值为80%
  val weightsOfOfflineShoppingDistrictIndex = 0.05

  private def finalScore(gpa: Double) = {
    gpa * 150 + 500
  }

  /** 获取规模得分 **/
  private def getScaleScore(scoreOfPayMoneyAnnAvgGPA: Double, scoreOfPerCigarAvgPriceOfAnnAvgGPA: Double) =
    finalScore((weightsOfPayMoneyAnnAvg * scoreOfPayMoneyAnnAvgGPA + weightsOfperCigarAvgPriceOfAnnAvg * scoreOfPerCigarAvgPriceOfAnnAvgGPA) / (weightsOfPayMoneyAnnAvg + weightsOfperCigarAvgPriceOfAnnAvg))

  /** 获取盈利得分 **/
  private def getProfitScore(weightsOfsalesRentRatioGPA: Double, weightsOfgrossMarginLastYearGPA: Double) =
    finalScore((weightsOfsalesRentRatio * weightsOfsalesRentRatioGPA + weightsOfgrossMarginLastYear * weightsOfgrossMarginLastYearGPA) / (weightsOfsalesRentRatio + weightsOfgrossMarginLastYear))

  /** 获取成长得分 **/
  private def getGrowingUpScore(monthlySalesGrowthRatioGPA: Double) = finalScore(monthlySalesGrowthRatioGPA)

  /** 获取运营得分 **/
  private def getOperationsScore(orderAmountAnnAvgGPA: Double, monthsNumsFromEarliestOrderGPA: Double, activeCategoryInLastMonthGPA: Double, categoryConcentrationGPA: Double) =
    finalScore((weightsOfOrderAmountAnnAvg * orderAmountAnnAvgGPA + weightsOfMonthsNumsFromEarliestOrder * monthsNumsFromEarliestOrderGPA + weightsOfActiveCategoryInLastMonth * activeCategoryInLastMonthGPA + weightsOfCategoryConcentration * categoryConcentrationGPA) / (weightsOfOrderAmountAnnAvg + weightsOfMonthsNumsFromEarliestOrder + weightsOfActiveCategoryInLastMonth + weightsOfCategoryConcentration))

  /** 获取市场得分 **/
  private def getMarketScore(offlineShoppingDistrictIndexGPA: Double) = finalScore(offlineShoppingDistrictIndexGPA)

  /** 总评分 **/
  private def getTotalScore(scoreOfPayMoneyAnnAvgGPA: Double, scoreOfPerCigarAvgPriceOfAnnAvgGPA: Double, weightsOfsalesRentRatioGPA: Double, weightsOfgrossMarginLastYearGPA: Double, monthlySalesGrowthRatioGPA: Double, orderAmountAnnAvgGPA: Double, monthsNumsFromEarliestOrderGPA: Double, activeCategoryInLastMonthGPA: Double, categoryConcentrationGPA: Double, offlineShoppingDistrictIndexGPA: Double) = {
    finalScore((weightsOfPayMoneyAnnAvg * scoreOfPayMoneyAnnAvgGPA + weightsOfperCigarAvgPriceOfAnnAvg * scoreOfPerCigarAvgPriceOfAnnAvgGPA + weightsOfsalesRentRatio * weightsOfsalesRentRatioGPA + weightsOfgrossMarginLastYear * weightsOfgrossMarginLastYearGPA + weightsOfMonthlySalesGrowthRatio * monthlySalesGrowthRatioGPA + weightsOfOrderAmountAnnAvg * orderAmountAnnAvgGPA + weightsOfMonthsNumsFromEarliestOrder * monthsNumsFromEarliestOrderGPA + weightsOfActiveCategoryInLastMonth * activeCategoryInLastMonthGPA + weightsOfCategoryConcentration * categoryConcentrationGPA + weightsOfOfflineShoppingDistrictIndex * offlineShoppingDistrictIndexGPA))
  }
  
  private def calcPayMoneyAnnAvgGPA(value: Double) = rangeOfGPA((value - 50000) / 100000)

  private def calcPerCigarAvgPriceOfAnnAvgGPA(value: Double) = rangeOfGPA((value - 120) / 120)

  private def calcSalesRentRatioGPA(value: Double) = rangeOfGPA(value / 10)

  private def calcGrossMarginLastYearGPA(value: Double) = rangeOfGPA((value - 0.14) / 0.03)

  private def calcMonthlySalesGrowthRatioGPA(value: Double) = rangeOfGPA(value / 1.25)

  private def calcOrderAmountAnnAvgGPA(value: Double) = rangeOfGPA((value - 300) / 300)

  private def calcMonthsNumsFromEarliestOrderGPA(value: Double) = rangeOfGPA((value - 12) / 6)

  private def calcActiveCategoryInLastMonthGPA(value: Double) = rangeOfGPA((value - 20) / 20)

  private def calcCategoryConcentrationGPA(value: Double) = rangeOfGPA((value - 0.7) / 0.3)

  private def calcOfflineShoppingDistrictIndexGPA(value: Double) = rangeOfGPA(value)

  /**
   * 获取规模相关的绩点
   * 返回：(licenseNo,(payMoneyAnnAvgGPA,perCigarAvgPriceOfAnnAvgGPA))
   * 中文：(许可证号  ,(   订货额年均值绩点,            每条均价年均值绩点))
   */
  private def getScaleGPA = {
    val payMoneyAnnAvgGPA = ScoreDao.payMoneyAnnAvg
      .map(t => (t._1, ScoreService.calcPayMoneyAnnAvgGPA(t._2)))
    val perCigarAvgPriceOfAnnAvgGPA = ScoreDao.perCigarAvgPriceOfAnnAvg
      .map(t => (t._1, ScoreService.calcPerCigarAvgPriceOfAnnAvgGPA(t._2)))
    payMoneyAnnAvgGPA.leftOuterJoin(perCigarAvgPriceOfAnnAvgGPA).map(t=> (t._1,(t._2._1,t._2._2.get)))
  }

  /**
   * 获取盈利相关的绩点
   * 返回：(licenseNo,(salesRentRatioGPA,grossMarginLastYearGPA))
   * 中文：(许可证号  ,(   销售额租金比绩点,           1年毛利率绩点))
   */
  private def getProfitGPA = {
    val salesRentRatioGPA = ScoreDao.salesRentRatio
      .map(t => (t._1, ScoreService.calcSalesRentRatioGPA(t._2)))
    val grossMarginLastYearGPA = ScoreDao.grossMarginLastYear
      .map(t => (t._1, ScoreService.calcGrossMarginLastYearGPA(t._2)))
    salesRentRatioGPA.leftOuterJoin(grossMarginLastYearGPA).map(t=> (t._1,(t._2._1,t._2._2.get)))
  }

  /**
   * 获取成长相关的绩点
   * 返回：(licenseNo, monthlySalesGrowthRatioGPA)
   * 中文：(许可证号  ,             月销售增长比绩点)
   */
  private def getGrowingUpGPA = {
    val monthlySalesGrowthRatioGPA = ScoreDao.monthlySalesGrowthRatio
      .map(t => (t._1, ScoreService.calcMonthlySalesGrowthRatioGPA(t._2)))
    monthlySalesGrowthRatioGPA
  }

  /**
   * 获取运营相关的绩点
   *
   * 返回：(licenseNo, (orderAmountAnnAvgGPA,monthsNumsFromEarliestOrderGPA,activeCategoryInLastMonthGPA,categoryConcentrationGPA))
   * 中文：(许可证号  , (    订货条数年均值绩点,                    经营期限绩点,                  活跃品类绩点,            品类集中度绩点))
   */
  private def getOperationGPA = {
    val orderAmountAnnAvgGPA = ScoreDao.orderAmountAnnAvg
      .map(t => (t._1, ScoreService.calcOrderAmountAnnAvgGPA(t._2)))
    val monthsNumsFromEarliestOrderGPA = ScoreDao.monthsNumsFromEarliestOrder
      .map(t => (t._1, ScoreService.calcMonthsNumsFromEarliestOrderGPA(t._2)))
    val activeCategoryInLastMonthGPA = ScoreDao.getActiveCategoryInLastMonth
      .map(t => (t._1, ScoreService.calcActiveCategoryInLastMonthGPA(t._2)))
    val categoryConcentrationGPA = ScoreDao.categoryConcentration
      .map(t => (t._1, ScoreService.calcCategoryConcentrationGPA(t._2)))
    val tempOperationsGPA1 = orderAmountAnnAvgGPA.leftOuterJoin(monthsNumsFromEarliestOrderGPA).map(t=> (t._1,(t._2._1,t._2._2.get)))
    val tempOperationsGPA2 = activeCategoryInLastMonthGPA.leftOuterJoin(categoryConcentrationGPA).map(t=> (t._1,(t._2._1,t._2._2.get)))
    tempOperationsGPA1.leftOuterJoin(tempOperationsGPA2).map(t=> (t._1,(t._2._1._1,t._2._1._2,t._2._2.get._1,t._2._2.get._2)))
  }

  /**
   * 获取市场相关的绩点
   * 返回：(licenseNo, offlineShoppingDistrictIndexGPA)
   * 中文：(许可证号  ,                      线下商圈指数)
   */
  private def getMarketGPA = {
    val offlineShoppingDistrictIndexGPA = ScoreDao.offlineShoppingDistrictIndex
      .map(t => (t._1, ScoreService.calcOfflineShoppingDistrictIndexGPA(t._2)))
    offlineShoppingDistrictIndexGPA
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
class ScoreService extends DataSource {

  def run(): Unit = {
    try {
      logInfo(Utils.wrapLog("开始运行评分模型"))
    /*  val payMoneyAnnAvgGPA = ScoreDao.payMoneyAnnAvg
        .map(t => (t._1, ScoreService.calcPayMoneyAnnAvgGPA(t._2)))
      val perCigarAvgPriceOfAnnAvgGPA = ScoreDao.perCigarAvgPriceOfAnnAvg
        .map(t => (t._1, ScoreService.calcPerCigarAvgPriceOfAnnAvgGPA(t._2)))
     payMoneyAnnAvgGPA.leftOuterJoin(perCigarAvgPriceOfAnnAvgGPA).map(t=> (t._1,(t._2._1,t._2._2.get))) // (licenseNo,(payMoneyAnnAvgGPA,perCigarAvgPriceOfAnnAvgGPA))
   */
     val scaleGPA = ScoreService.getScaleGPA
      /*
            val salesRentRatioGPA = ScoreDao.salesRentRatio
              .map(t => (t._1, ScoreService.calcSalesRentRatioGPA(t._2)))
            val grossMarginLastYearGPA = ScoreDao.grossMarginLastYear
              .map(t => (t._1, ScoreService.calcGrossMarginLastYearGPA(t._2)))
             salesRentRatioGPA.leftOuterJoin(grossMarginLastYearGPA).map(t=> (t._1,(t._2._1,t._2._2.get)))// (licenseNo,(salesRentRatioGPA,grossMarginLastYearGPA))
           */
    val profitGPA = ScoreService.getProfitGPA

    val growingUp = ScoreService.getGrowingUpGPA

      val  operationGPA = ScoreService.getOperationGPA

      /*      val orderAmountAnnAvgGPA = ScoreDao.orderAmountAnnAvg
        .map(t => (t._1, ScoreService.calcOrderAmountAnnAvgGPA(t._2)))
      val monthsNumsFromEarliestOrderGPA = ScoreDao.monthsNumsFromEarliestOrder
        .map(t => (t._1, ScoreService.calcMonthsNumsFromEarliestOrderGPA(t._2)))
      val activeCategoryInLastMonthGPA = ScoreDao.getActiveCategoryInLastMonth
        .map(t => (t._1, ScoreService.calcActiveCategoryInLastMonthGPA(t._2)))
      val categoryConcentrationGPA = ScoreDao.categoryConcentration
        .map(t => (t._1, ScoreService.calcCategoryConcentrationGPA(t._2)))
      val tempOperationsGPA1 = orderAmountAnnAvgGPA.leftOuterJoin(monthsNumsFromEarliestOrderGPA).map(t=> (t._1,(t._2._1,t._2._2.get)))
      val tempOperationsGPA2 = activeCategoryInLastMonthGPA.leftOuterJoin(categoryConcentrationGPA).map(t=> (t._1,(t._2._1,t._2._2.get)))
      val operationsGPA = tempOperationsGPA1.leftOuterJoin(tempOperationsGPA2).map(t=> (t._1,(t._2._1._1,t._2._1._2,t._2._2.get._1,t._2._2.get._2)))*/

      val offlineShoppingDistrictIndexGPA = ScoreDao.offlineShoppingDistrictIndex
        .map(t => (t._1, ScoreService.calcOfflineShoppingDistrictIndexGPA(t._2)))



      //FileUtils.saveAsTextFile(null, Constants.OutputPath.LOAN_WARNING_TEXT)
    } catch {
      case t: Throwable =>
        MailAgent(t, Constants.Mail.SCORE_SUBJECT).sendMessage()
        logError(Constants.Mail.SCORE_SUBJECT, t)
    } finally {
      logInfo(Utils.wrapLog("评分模型运行结束"))
    }
  }
}


