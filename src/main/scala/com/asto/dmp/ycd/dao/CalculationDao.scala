package com.asto.dmp.ycd.dao

import com.asto.dmp.ycd.base.SQL
import com.asto.dmp.ycd.util.{DateUtils, BizUtils}

/**
 * 指标计算
 */
object CalculationDao {
  /**
   * 经营期限（月）= 申请贷款月份(系统运行时间) - 最早一笔网上订单的月份
   */
  def monthsNumsFromEarliestOrder() = {
    BizDao.getOrderDetailsProps(SQL().select("license_no,order_date"))
      .map(a => (a(0).toString, a(1).toString))
      .groupByKey()
      .map(t => (t._1, BizUtils.monthsNumsFrom(t._2.min, "yyyy-MM-dd")))
  }

  /**
   * 订货额年均值 = 近12个月（不含贷款当前月）“金额”字段，金额之和/12
   * 返回的元素,如：(33010120120716288A,68260)
   */
  def payMoneyAnnualAverage() = lastMonthsAverage("license_no,pay_money", 12)

  /**
   * 订货条数年均值 = 近12个月（不含贷款当前月）“订货量”字段，订货量之和/12、
   * 返回的元素,如：(33010120120716288A,427)
   */
  def orderAmountAnnualAverage() = lastMonthsAverage("license_no,order_amount", 12)

  /**
   * 计算年均值。订货额年均值和订货条数年均值的计算过程基本相同，除了第二个字段不同，所以提取出计算逻辑。
   * @param fields
   * @return
   */
  private def lastMonthsAverage(fields: String, backMonthsNum: Int) = {
    lastMonthsSum(fields, backMonthsNum).map(t => (t._1, t._2 / backMonthsNum))
  }

  /**
   * 计算年总额。
   * 计算近12月总提货额(传入"license_no,pay_money")
   * 计算近12月总进货条数(传入"license_no,order_amount")
   * @param fields
   * @return
   */
  private def lastMonthsSum(fields: String, backMonthsNum: Int) = {
    BizDao.getOrderDetailsProps(
      SQL().select(fields).
        where(s" order_date >= '${DateUtils.monthsAgo(backMonthsNum, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
    )
    .map(a => (a(0).toString, a(1).toString.toDouble))
    .groupByKey()
    .map(t => (t._1, t._2.sum.toInt)).cache()
  }

  /**
   * 每条均价年均值 = 近12月总提货额 / 近12月总进货条数
   */
  def perCigaretteAveragePriceOfAnnualAverage = {
    lastMonthsSum("license_no,pay_money", 12)
    .leftOuterJoin(lastMonthsAverage("license_no,order_amount", 12)) //(33010120120716288A,(819131,Some(427)))
    .filter(t => t._2._2.isDefined && t._2._2.get.toDouble > 0)
    .map(t => (t._1, t._2._1 / t._2._2.get))
  }

  /**
   * 活跃品类数
   */
  def numberOfActiveCategory = {
    // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 业务暂时不明白 ～～～～～～～～～～～～～～～～～～～～
  }

  /**
   * 单品毛利率
   */
  def grossMarginInSingleCategory = {

  }

  /**
   * 客户各品类毛利率
   */
  def grossMarginInShop = {

  }

  /**
   * 月销售增长比 = 近3月月平均销售 / 近6月月平均销售 (不足六个月怎么算？)
   * 返回的数据保留两位小数
   */
  def monthlySalesGrowthRatio = {
    lastMonthsAverage("license_no,pay_money", 3)
    .leftOuterJoin(lastMonthsAverage("license_no,pay_money", 6)) //(33010102981025009A,(130263,Some(125425)))
    .map(t => (t._1, f"${(t._2._1.toDouble / t._2._2.get)}%1.2f"))
  }
}
