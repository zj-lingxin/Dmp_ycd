package com.asto.dmp.ycd.dao.impl

import com.asto.dmp.ycd.base.{Constants, Contexts}
import com.asto.dmp.ycd.dao.SQL
import com.asto.dmp.ycd.util.{BizUtils, DateUtils, Utils}

object BizDao {

  /**
   * 经营期限（月）= 申请贷款月份(系统运行时间) - 最早一笔网上订单的月份
   */
  def monthsNumFromEarliestOrder = {
    BaseDao.getOrderProps(SQL().select("store_id,order_date"))
      .map(a => (a(0).toString, a(1).toString))
      .groupByKey()
      .map(t => (t._1, BizUtils.monthsNumFrom(t._2.min, "yyyy-MM-dd"))).persist()
  }

  /**
   * 订货额年均值 = 近12个月（不含贷款当前月）“金额”字段，金额之和/12
   * 返回的元素,如：(33010120120716288A,68260)
   */
  def moneyAmountAnnAvg = lastMonthsAvg("store_id,money_amount", 12).persist()

  /**
   * 订货条数年均值 = 近12个月（不含贷款当前月）“订货量”字段，订货量之和/12、
   * 返回的元素,如：(33010120120716288A,427)
   */
  def orderAmountAnnAvg = lastMonthsAvg("store_id,order_amount", 12).persist()

  /**
   * 计算年均值。订货额年均值和订货条数年均值的计算过程基本相同，除了第二个字段不同，所以提取出计算逻辑。
   */
  private def lastMonthsAvg(fields: String, backMonthsNum: Int) = {
    lastMonthsSum(fields, backMonthsNum)
      .map(t => (t._1, t._2 / backMonthsNum))
  }

  /**
   * 计算年总额。
   * 计算近12月总提货额(传入"store_id,money_amount")
   * 计算近12月总进货条数(传入"store_id,order_amount")
   */
  private def lastMonthsSum(fields: String, backMonthsNum: Int) = {
    selectLastMonthsData(fields, backMonthsNum)
      .map(a => (a(0).toString, a(1).toString.toDouble))
      .groupByKey()
      .map(t => (t._1, t._2.sum.toInt)).cache()
  }

  private def selectLastMonthsData(fields: String, backMonthsNum: Int) = {
    BaseDao.getOrderProps(
      SQL().select(fields).
        where(s" order_date >= '${DateUtils.monthsAgo(backMonthsNum, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
    )
  }

  /**
   * 每条均价年均值 = 近12月总提货额 / 近12月总进货条数
   */
  def perCigarAvgPriceOfAnnAvg = {
    lastMonthsSum("store_id,money_amount", 12)
      .leftOuterJoin(lastMonthsSum("store_id,order_amount", 12))
      .filter(t => t._2._2.isDefined && t._2._2.get.toDouble > 0)
      .map(t => (t._1, t._2._1 / t._2._2.get)).persist()
  }

  /**
   * 活跃品类数
   * 当月月均活跃品类，为前三个月月均的订货量≥3的品类之和，如2015.10显示的活跃品类为，2015.8-2015.10三个月订货量≥9的，品类数之和；
   * 计算12个月的活跃评类数。如果不能提取到14个月的数据，则最初两个月的数据为空，均值按近十个月计算；
   * 返回：店铺id，日期，活跃品类数
   * 注意：该方法的效率比较低，如果对系统执行时间有影响，可以对该方法进行优化。
   */
  def getActiveCategoryInLast12Months = {
    val list = scala.collection.mutable.ListBuffer[(String, String, Long)]()
    storeIdArray.foreach(list ++= getActiveCategoryFor(_, (0,11)))
    Contexts.sparkContext.parallelize(list)
  }

  /**
   * 近12个月，每个月的订货额
   */
  def moneyAmountPerMonth = {
    selectLastMonthsData(s"order_date,money_amount", 12)
      .map(a => (DateUtils.cutYearMonth(a(0).toString),a(1).toString.toDouble))
      .groupByKey()
      .map(t => (t._1, Utils.retainDecimal(t._2.sum, 2))).cache()
  }

  /**
   * 近12个月，每个月的订货条数
   */
  def orderAmountPerMonth = {
    selectLastMonthsData(s"order_date,order_amount", 12)
      .map(a => (DateUtils.cutYearMonth(a(0).toString),a(1).toString.toInt))
      .groupByKey()
      .map(t => (t._1, t._2.sum)).cache()
  }

  /**
   * 近12个月，每个月的订货品类数
   */
  def categoryPerMonth = {
    selectLastMonthsData("order_date,cigar_name", 12).map(a => (DateUtils.cutYearMonth(a(0).toString),a(1).toString)).distinct().map(t => t._1).countByValue().toList.sorted.reverse
  }

  /**
   * 近12个月，每个月的订货次数
   */
  def orderNumberPerMonth = {
    selectLastMonthsData("order_date,order_id", 12).map(a => (DateUtils.cutYearMonth(a(0).toString),a(1).toString)).distinct().map(t => t._1).countByValue().toList.sorted.reverse
  }

  /**
   * 近12个月，每条均价
   */
  def perCigarPricePerMonth = {
    moneyAmountPerMonth.leftOuterJoin(orderAmountPerMonth).map(t => (t._1, Utils.retainDecimal(t._2._1 / t._2._2.get, 2))).collect().toList.sorted.reverse
  }

  def getActiveCategoryFor(storeId: String, timeRange:(Int,Int)) = {
    println("~~~~~~~~~~~~~~~~~~~~~~ once ~~~~~~~~~~~~~~~~~~~~~~~~~~")
    val monthsNumFromEarliestOrderMap = monthsNumFromEarliestOrder.collect().toMap[String, Int]
    val monthNum = Math.min(monthsNumFromEarliestOrderMap(storeId), 14)
    val list = scala.collection.mutable.ListBuffer[(String, String, Long)]()
    (timeRange._1 to timeRange._2).toStream.takeWhile(m => (m + 3) <= monthNum).foreach {
      m => list += Tuple3(storeId, DateUtils.monthsAgo(m, "yyyy-MM"), countNumbersOfActiveCategoryForMonth(m, storeId))
    }
    list
  }

  /**
   * 获取当月活跃品类数
   */
  def getActiveCategoryInLastMonth = {
    BaseDao.getOrderProps(SQL().select("store_id,cigar_name,order_date,order_amount").where(s" order_date >= '${DateUtils.monthsAgo(3, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'"))
      .map(a => ((a(0).toString,a(1).toString), a(3).toString.toInt))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .filter(t => t._2 >= 9)
      .map(t => (t._1._1,1))
      .groupByKey()
      .map(t => (t._1,t._2.sum)).persist()
  }

   def countNumbersOfActiveCategoryForMonth(m: Int, storeId: String) = {
    BaseDao.getOrderProps(SQL().select("cigar_name,order_date,order_amount").where(s" order_date >= '${DateUtils.monthsAgo(m + 3, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(m, "yyyy-MM-01")}' and store_id = '$storeId'"))
      .map(a => (a(0).toString, a(2).toString.toInt))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .filter(t => t._2 >= 9).count()
  }

  /**
   * 单品毛利率 = （零售指导价-成本价）/指导价*100%
   * 按月计算
   * 返回：店铺id，年月，卷烟牌子，毛利率
   */
  def grossMarginPerMonthCategory = {
    val array = BaseDao.getOrderProps(SQL().select("store_id,order_date,money_amount,order_amount,retail_price,cigar_name"))
      .map(a => ((a(0).toString, a(1).toString.substring(0, 7), a(5).toString), (a(2).toString.toDouble, a(3).toString.toInt * a(4).toString.toDouble)))
      .groupByKey()
      .map(t => (t._1, t._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))))
      .filter(t => t._2._1.toInt > 0 && t._2._2.toInt > 0)
      .map(t => ((t._1._1,t._1._2, Utils.retainDecimal(1 - t._2._1 / t._2._2, 3)),t._1._3))
      .collect()
      .sortWith((a,b) => a._1.toString() > b._1.toString())
      .map(t => (t._1._1, t._1._2, t._2, t._1._3))
    Contexts.sparkContext.parallelize(array)
  }

  /**
   * 客户各品类毛利率
   * 毛利率=毛利/销售金额=（销售金额-成本(进货额））/销售金额=1-成本（money_amount）/销售金额（order_amount*retail_price）
   * 按月计算
   * 返回：店铺id，日期，每月毛利率
   */
  def grossMarginPerMonthAll = {
    val array = BaseDao.getOrderProps(SQL().select("store_id,order_date,money_amount,order_amount,retail_price"))
      .map(a => ((a(0).toString, a(1).toString.substring(0, 7)), (a(2).toString.toDouble, a(3).toString.toInt * a(4).toString.toDouble)))
      .groupByKey()
      .map(t => (t._1, t._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))))
      .map(t => (t._1._1, t._1._2, Utils.retainDecimal(1 - t._2._1 / t._2._2, 3))) //((33010220120807247A,2015-01),0.18)
      .collect().sortWith((a,b) => a.toString() > b.toString())
    Contexts.sparkContext.parallelize(array)
  }

  /**
   * 近一年毛利率
   * @return
   */
  def grossMarginLastYear = {
    BaseDao.getOrderProps(
      SQL()
        .select("store_id,money_amount,order_amount,retail_price")
        .where(s" order_date >= '${DateUtils.monthsAgo(12, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
    )
      .map(a => (a(0), (a(1).toString.toDouble, a(2).toString.toInt * a(3).toString.toDouble)))
      .groupByKey()
      .map(t => (t._1, t._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))))
      .map(t => (t._1.toString, Utils.retainDecimal(1 - t._2._1 / t._2._2, 3))).persist()
  }

  /**
   * 月销售增长比 = 近3月月平均销售 / 近6月月平均销售 (不足六个月怎么算？)
   * 返回的数据保留两位小数
   */
  def monthlySalesGrowthRatio = {
    lastMonthsAvg("store_id,money_amount", 3)
      .leftOuterJoin(lastMonthsActualAvg(6)) //(33010102981025009A,(130263,Some(125425)))
      .map(t => (t._1, Utils.retainDecimal(t._2._1.toDouble / t._2._2.get))).persist()
  }

  /**
   * 如果实际月数小于monthsNum，那么取实际的那个月份数
   */
  private def lastMonthsActualAvg(monthsNum: Int) = {
    lastMonthsSum("store_id,money_amount", monthsNum)
      .leftOuterJoin(monthsNumFromEarliestOrder)
      .map(t => (t._1, t._2._1, Math.min(t._2._2.getOrElse(0), monthsNum)))
      .map(t => (t._1, t._2 / t._3))
  }

  /**
   * 品类集中度：近12月销售额最高的前10名的销售额占总销售额的比重，TOP10商品的销售额之和/总销售额（近12月）
   */
  def categoryConcentration = {
    Contexts.sparkContext
      .parallelize(getTop10Category)
      .leftOuterJoin(getLast12MonthsSales)
      .map(t => (t._1, t._2._1._1 / t._2._2.get))
      .groupByKey()
      .map(t => (t._1, Utils.retainDecimal(t._2.sum))).persist()
  }

  private def getTop10Category = {
    val array = getAllCategoryConcentration
    var topIndex: Int = 0
    val list = scala.collection.mutable.ListBuffer[(String, (Double, String))]()
    storeIdArray.foreach {
      licenseNo =>
        for (a <- array if a._1 == licenseNo && topIndex < 10) {
          list += a
          topIndex += 1
        }
        topIndex = 0
    }
    list
  }

  private def getAllCategoryConcentration = {
    selectLastMonthsData("store_id,cigar_name,order_amount,retail_price", 12)
      .map(a => (a(0).toString, a(1).toString, a(2).toString.toInt * a(3).toString.toDouble))
      .map(t => ((t._1, t._2), t._3))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .map(t => ((t._1._1, t._2), t._1._2))
      .sortByKey(ascending = false).map(t => (t._1._1, (t._1._2, t._2))).collect() //((33010220120807247A,300.0),七匹狼(蓝))
  }

  private def getLast12MonthsSales = {
    selectLastMonthsData("store_id,order_amount,retail_price", 12)
      .map(a => (a(0).toString, a(1).toString.toInt * a(2).toString.toDouble))
      .map(t => (t._1, t._2))
      .groupByKey()
      .map(t => (t._1, t._2.sum)) //((33010220120807247A,300.0),七匹狼(蓝))
  }

  /**
   * 销售额租金比
   * 暂时缺失数据，默认为0.6
   */
  def salesRentRatio = {
    Contexts.sparkContext.parallelize(storeIdArray).map((_,0.6)).persist()
  }

  def storeIdArray = BaseDao.getOrderProps(SQL().select("store_id")).map(a => a(0).toString).distinct().collect()
  /**
   * 线下商圈指数
   * 暂时缺失数据，默认为0.8
   */
  def offlineShoppingDistrictIndex = {
    Contexts.sparkContext.parallelize(storeIdArray).map((_,0.8D)).persist()
  }

}
