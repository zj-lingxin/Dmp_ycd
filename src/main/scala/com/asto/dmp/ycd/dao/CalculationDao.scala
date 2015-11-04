package com.asto.dmp.ycd.dao

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.util.{DateUtils, BizUtils}

/**
 * 指标计算
 */
object CalculationDao {
  private val licenseNoArray = BizDao.getFullFieldsOrderProps(SQL().select("license_no")).map(a => a(0).toString).distinct().collect()

  /**
   * 经营期限（月）= 申请贷款月份(系统运行时间) - 最早一笔网上订单的月份
   */
  def monthsNumsFromEarliestOrder() = {
    BizDao.getFullFieldsOrderProps(SQL().select("license_no,order_date"))
      .map(a => (a(0).toString, a(1).toString))
      .groupByKey()
      .map(t => (t._1, BizUtils.monthsNumsFrom(t._2.min, "yyyy-MM-dd"))).cache()
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
   */
  private def lastMonthsAverage(fields: String, backMonthsNum: Int) = {
    lastMonthsSum(fields, backMonthsNum).map(t => (t._1, t._2 / backMonthsNum))
  }

  /**
   * 计算年总额。
   * 计算近12月总提货额(传入"license_no,pay_money")
   * 计算近12月总进货条数(传入"license_no,order_amount")
   */
  private def lastMonthsSum(fields: String, backMonthsNum: Int) = {
    selectLastMonthsData(fields, backMonthsNum)
      .map(a => (a(0).toString, a(1).toString.toDouble))
      .groupByKey()
      .map(t => (t._1, t._2.sum.toInt)).cache()
  }

  private def selectLastMonthsData(fields: String, backMonthsNum: Int) = {
    BizDao.getFullFieldsOrderProps(
      SQL().select(fields).
        where(s" order_date >= '${DateUtils.monthsAgo(backMonthsNum, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
    )
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
   * 当月月均活跃品类，为前三个月月均的订货量≥3的品类之和，如2015.10显示的活跃品类为，2015.8-2015.10三个月订货量≥9的，品类数之和；
   * 计算12个月的活跃评类数。如果不能提取到14个月的数据，则最初两个月的数据为空，均值按近十个月计算；
   */
  def getActiveCategoryForEveryLicenseNo = {
    val list = scala.collection.mutable.ListBuffer[(String, String, Long)]()
    licenseNoArray.foreach(list ++= getActiveCategoryInLast12MonthsFor(_))
    list
  }

  private def getActiveCategoryInLast12MonthsFor(licenseNo: String) = {
    val monthsNumsFromEarliestOrderMap = monthsNumsFromEarliestOrder().collect().toMap[String, Int]
    val monthNums = Math.min(monthsNumsFromEarliestOrderMap(licenseNo), 14)
    val list = scala.collection.mutable.ListBuffer[(String, String, Long)]()
    (0 to 11).toStream.takeWhile(m => (m + 3) <= monthNums).foreach {
      m => list += Tuple3(licenseNo, DateUtils.monthsAgo(m, "yyyy-MM"), countNumbersOfActiveCategoryForMonth(m, licenseNo))
    }
    list
  }

  private def countNumbersOfActiveCategoryForMonth(m: Int, licenseNo: String) = {
    BizDao.getFullFieldsOrderProps(SQL().select("cigarette_name,order_date,order_amount").where(s" order_date >= '${DateUtils.monthsAgo(m + 3, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(m, "yyyy-MM-01")}' and license_no = '$licenseNo'"))
      .map(a => (a(0).toString, a(2).toString.toInt))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .filter(t => t._2 >= 9).count()
  }


  /**
   * 单品毛利率 = （零售指导价-成本价）/指导价*100%
   * 按月计算
   */
  def grossMarginInSingleCategory = {
    BizDao.getFullFieldsOrderProps(SQL().select("license_no,order_date,pay_money,order_amount,retail_price,cigarette_name"))
      .map(a => ((a(0).toString, a(1).toString.substring(0, 7), a(5).toString), (a(2).toString.toDouble, a(3).toString.toInt * a(4).toString.toDouble)))
      .groupByKey()
      .map(t => (t._1, t._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))))
      .filter(t => t._2._1.toInt > 0 && t._2._2.toInt > 0)
      .map(t => (t._1, BizUtils.retainTwoDecimal(1 - t._2._1 / t._2._2)))
  }

  /**
   * 客户各品类毛利率
   * 毛利率=毛利/销售金额=（销售金额-成本(进货额））/销售金额=1-成本（pay_money）/销售金额（order_amount*retail_price）
   * 按月计算
   */
  def grossMarginInShop = {
    BizDao.getFullFieldsOrderProps(SQL().select("license_no,order_date,pay_money,order_amount,retail_price"))
      .map(a => ((a(0).toString, a(1).toString.substring(0, 7)), (a(2).toString.toDouble, a(3).toString.toInt * a(4).toString.toDouble)))
      .groupByKey()
      .map(t => (t._1, t._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))))
      .map(t => (t._1, BizUtils.retainTwoDecimal(1 - t._2._1 / t._2._2))) //((33010220120807247A,2015-01),0.18)
  }

  /**
   * 月销售增长比 = 近3月月平均销售 / 近6月月平均销售 (不足六个月怎么算？)
   * 返回的数据保留两位小数
   */
  def monthlySalesGrowthRatio = {
    lastMonthsAverage("license_no,pay_money", 3)
      .leftOuterJoin(lastMonthsActualAverage(6)) //(33010102981025009A,(130263,Some(125425)))
      .map(t => (t._1, BizUtils.retainTwoDecimal(t._2._1.toDouble / t._2._2.get)))
  }

  /**
   * 如果实际月数小于monthsNums，那么取实际的那个月份数
   */
  private def lastMonthsActualAverage(monthsNums: Int) = {
    lastMonthsSum("license_no,pay_money", monthsNums)
      .leftOuterJoin(monthsNumsFromEarliestOrder())
      .map(t => (t._1, t._2._1, Math.min(t._2._2.getOrElse(0), monthsNums)))
      .map(t => (t._1, {
      (t._2 / t._3)
    }))
  }

  /**
   * 品类集中度：近12月销售额最高的前10名的销售额占总销售额的比重，TOP10商品对应销售额/总销售额（近12月）
   */
  def categoryConcentration = {
    Contexts.getSparkContext
      .parallelize(getTop10CategoryForEachLicenseNo)
      .leftOuterJoin(getLast12MonthsSales)
      .map(t =>((t._1, BizUtils.retainTwoDecimal(t._2._1._1 / t._2._2.get), t._2._1._2)))

  }

  private def getTop10CategoryForEachLicenseNo = {
    val array = getAllCategoryConcentration
    var topIndex: Int = 0
    val list = scala.collection.mutable.ListBuffer[(String, (Double, String))]()
    licenseNoArray.foreach {
      licenseNo =>
        for (a <- array if (a._1 == licenseNo && topIndex < 10)) {
          list += a
          topIndex += 1
        }
        topIndex = 0
    }
    list
  }

  private def getAllCategoryConcentration = {
    selectLastMonthsData("license_no,cigarette_name,order_amount,retail_price", 12)
      .map(a => (a(0).toString, a(1).toString, a(2).toString.toInt * a(3).toString.toDouble))
      .map(t => ((t._1, t._2), t._3))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .map(t => ((t._1._1, t._2), t._1._2))
      .sortByKey(false).map(t => (t._1._1, (t._1._2, t._2))).collect() //((33010220120807247A,300.0),七匹狼(蓝))
  }

  private def getLast12MonthsSales = {
    selectLastMonthsData("license_no,order_amount,retail_price", 12)
      .map(a => (a(0).toString, a(1).toString.toInt * a(2).toString.toDouble))
      .map(t => (t._1, t._2))
      .groupByKey()
      .map(t => (t._1, t._2.sum)) //((33010220120807247A,300.0),七匹狼(蓝))
  }
  // 城市,   许可证号,    订单号,  订货日期,       卷烟名称,   批发价,             要货量,      订货量, 成本|金额,   卷烟牌子     ,零售价（零售）  生产厂家
  //"city,license_no,order_id,order_date,cigarette_name,the_cost,need_goods_amount,order_amount,pay_money,cigarette_brand,retail_price,manufacturers"

}
