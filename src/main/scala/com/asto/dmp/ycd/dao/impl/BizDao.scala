package com.asto.dmp.ycd.dao.impl

import com.asto.dmp.ycd.base.Contexts
import com.asto.dmp.ycd.dao.SQL
import com.asto.dmp.ycd.util.{BizUtils, DateUtils, Utils}

object BizDao {
  private val maxCalcMonths = 12

  //店铺默认经营时间，单位：月
  private val defaultStoreAge = 18

  val storeIdCalcMonthsRDD = BaseDao.getStoreIdCalcMonthsProps().map(a => (a(0).toString, a(1).toString().toInt)).groupByKey().map(t => (t._1, t._2.min)).persist()

  val storeIdCalcMonthsArray = storeIdCalcMonthsRDD.collect()

  val storeIdCalcMonthsMap = storeIdCalcMonthsArray.toMap

  /**
   * 根据传入的storeId和orderDate，过滤掉不需要计算的数据.根据店铺所在区域不同，分别过滤出只需要计算近1、3、12个月的订单数据
   */
  private def filterData(storeId: String, orderDate: String): Boolean = {
    orderDate >= DateUtils.monthsAgo(storeIdCalcMonthsMap.get(storeId).getOrElse(1), "yyyy-MM-01")
  }

  /**
   * 经营期限（月）= 申请贷款月份(系统运行时间) - 最早一笔网上订单的月份
   * 只有1、3个月的数据的店铺的经营月份按照默认值defaultStoreAge计算
   */
  //已改
  def monthsNumFromEarliestOrder = {
    BaseDao.getOrderProps(SQL().select("store_id,order_date").where("order_date != 'null'"))
      .map(a => (a(0).toString, a(1).toString))
      .groupByKey()
      .map(t => (t._1, BizUtils.monthsNumFrom(t._2.min, "yyyy-MM-dd")))
      .leftOuterJoin(storeIdCalcMonthsRDD) //(e160d0221914444f9d8639c8234207cc,(26,Some(12)))
      .map(t => (t._1, if (t._2._2.getOrElse(1) == 12) t._2._1 else defaultStoreAge)).persist() //只有1、3个月的数据的店铺的经营月份按照默认值defaultStoreAge计算
  }

  //已改
  val avgFor = (property: String) => {
    BaseDao.getOrderProps(
      SQL().select(s"store_id,$property,order_date").
        where(s" order_date >= '${DateUtils.monthsAgo(maxCalcMonths, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
    ).filter(a => filterData(a(0).toString, a(2).toString))
      .map(a => (a(0).toString, a(1).toString.toDouble))
      .groupByKey()
      .map(t => (t._1, t._2.sum.toInt))
      .leftOuterJoin(storeIdCalcMonthsRDD) //(af46ef365bac42f88c5f5ecb46e555a5,(519,Some(1)))
      .map(t => (t._1, t._2._1 / t._2._2.getOrElse(1)))
  }

  /**
   * 订货额（年）均值 = 近n个月（不含贷款当前月）“金额”字段，金额之和/n
   * 返回的元素,如：(33010120120716288A,68260)
   */
  //已改
  val moneyAmountAnnAvg = avgFor("money_amount")

  /**
   * 订货条数(年)均值 = 近n个月（不含贷款当前月）“订货量”字段，订货量之和/n、
   * 返回的元素,如：(33010120120716288A,427)
   */
  //已改
  val orderAmountAnnAvg = avgFor("order_amount")

  /**
   * 月销售增长比(12个月数据版本) = 近3月平均销售/近6月平均销售
   * 月销售增长比(3个月数据版本) = 近1月销售/近3月平均销售额
   * 月销售增长比(1个月数据版本) 不进行计算
   * 返回的数据保留两位小数
   */
  //已改
  def monthlySalesGrowthRatio = {
    val monthsToCalcForFZ = (monthsNum: Int) => if (monthsNum == 12) 3 else if (monthsNum == 3) 1 else 1
    val monthsToCalcForFM = (monthsNum: Int) => if (monthsNum == 12) 6 else if (monthsNum == 3) 3 else 1

    val filterData = (storeId: String, orderDate: String, monthsNum: (Int, Int)) =>
      if (storeIdCalcMonthsMap.get(storeId).getOrElse(1) == 12)
        orderDate >= DateUtils.monthsAgo(monthsNum._1, "yyyy-MM-01")
      else if (storeIdCalcMonthsMap.get(storeId).getOrElse(1) == 3)
        orderDate >= DateUtils.monthsAgo(monthsNum._2, "yyyy-MM-01")
      else
        false

    val filterDataForMonthlySalesGrowthRatioFZ = (storeId: String, orderDate: String) => filterData(storeId, orderDate, (3, 1))
    val filterDataForMonthlySalesGrowthRatioFM = (storeId: String, orderDate: String) => filterData(storeId, orderDate, (6, 3))

    val computeRDD = (maxMonthsNum: Int, filterDataFun: (String,String) => Boolean, monthsToCalcFun: Int => Int) => {
      BaseDao.getOrderProps(
        SQL().select("store_id,money_amount,order_date").
          where(s" order_date >= '${DateUtils.monthsAgo(maxMonthsNum, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
      ).filter(a => filterDataFun(a(0).toString, a(2).toString))
        .map(a => (a(0).toString, a(1).toString.toDouble))
        .groupByKey()
        .map(t => (t._1, t._2.sum.toInt))
        .leftOuterJoin(storeIdCalcMonthsRDD) //(af46ef365bac42f88c5f5ecb46e555a5,(519,Some(1)))
        .map(t => (t._1, t._2._1 / monthsToCalcFun(t._2._2.getOrElse(1))))
    }

    val fzRDD = computeRDD(3,filterDataForMonthlySalesGrowthRatioFZ, monthsToCalcForFZ)
    val fmRDD = computeRDD(6,filterDataForMonthlySalesGrowthRatioFM, monthsToCalcForFM)

    fzRDD.leftOuterJoin(fmRDD).map(t => (t._1, Utils.retainDecimal(t._2._1.toDouble / t._2._2.get)))
  }

  /**
   * 每条均价(年)均值(12个月数据版本) = 近12月总提货额 / 近12月总进货条数 = (近12月总提货额/12) / (近12月总进货条数/12)
   * 每条均价(年)均值(3个月数据版本) = 近3月总提货额 / 近3月总进货条数
   * 每条均价(年)均值(1个月数据版本) = 近1月总提货额 / 近1月总进货条数
   */
  //已改
  def perCigarAvgPriceOfAnnAvg = {
    moneyAmountAnnAvg.leftOuterJoin(orderAmountAnnAvg).filter(t => t._2._2.isDefined && t._2._2.get.toDouble > 0)
      .map(t => (t._1, t._2._1 / t._2._2.get))
  }

  /**
   * 近n个月，每个月的订货额(n=1,3,12)
   */
  def moneyAmountPerMonth = {
    import Helper.storeIdAndOrderDateOrdering

    BaseDao.getOrderProps(
      SQL().select("store_id,money_amount,order_date").
        where(s" order_date >= '${DateUtils.monthsAgo(maxCalcMonths, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
    ).filter(a => filterData(a(0).toString, a(2).toString))
      .map(a => ((a(0).toString, DateUtils.strToStr(a(2).toString, "yyyy-MM-dd", "yyyyMM")), a(1).toString.toDouble))
      .groupByKey()
      .map(t => (t._1, Utils.retainDecimal(t._2.sum, 2))).sortBy(_._1).cache()
  }

  /**
   * 近12个月，每个月的订货条数
   */
  def orderAmountPerMonthNew = {
    import Helper.storeIdAndOrderDateOrdering
    selectLastMonthsData(s"store_id,order_date,order_amount", 12)
      .map(a => ((a(0).toString, DateUtils.strToStr(a(1).toString, "yyyy-MM-dd", "yyyyMM")), a(2).toString.toInt))
      .groupByKey()
      .map(t => (t._1, t._2.sum)).sortBy(_._1).cache()
  }

  def orderAmountPerMonth = {
    import Helper.storeIdAndOrderDateOrdering
    selectLastMonthsData(s"store_id,order_date,order_amount", 12)
      .map(a => ((a(0).toString, DateUtils.strToStr(a(1).toString, "yyyy-MM-dd", "yyyyMM")), a(2).toString.toInt))
      .groupByKey()
      .map(t => (t._1, t._2.sum)).sortBy(_._1).cache()
  }


  private def selectLastMonthsData(fields: String, backMonthsNum: Int) = {
    BaseDao.getOrderProps(
      SQL().select(fields).
        where(s" order_date >= '${DateUtils.monthsAgo(backMonthsNum, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
    )
  }

  /**
   * 活跃品类数
   * 当月月均活跃品类，为前三个月月均的订货量≥3的品类之和，如2015.10显示的活跃品类为，2015.8-2015.10三个月订货量≥9的，品类数之和；
   * 计算12个月的活跃评类数。如果不能提取到14个月的数据，则最初两个月的数据为空，均值按近十个月计算；
   * 返回：店铺id，日期，活跃品类数
   * 注意：该方法的效率比较低，如果对系统执行时间有影响，可以对该方法进行优化。
   */
  def getNewActiveCategoryInLast12Months(timeRanges: (Int, Int)) = {
    val storeIds = storeIdArray
    val allArray = allData.collect()
    val result = scala.collection.mutable.ListBuffer[(String, String, Int)]()
    storeIds.foreach {
      storeId =>
        (timeRanges._1 to timeRanges._2).foreach {
          months =>
            val thisMonths = DateUtils.monthsAgo(months, "yyyyMM")
            val range = monthRange(months)
            val num = allArray.filter(t => t._1._1 == storeId && range._1 >= t._1._2 && t._1._2 >= range._2)
              .map(t => (t._1._3, t._2))
              .groupBy(_._1)
              .map(t => (t._1, (for (e <- t._2.toList) yield e._2).sum))
              .filter(_._2 >= 9).toList.size
            result += Tuple3(storeId, thisMonths, num)
        }
    }
    result
  }

  private def monthRange(m: Int) = {
    (DateUtils.monthsAgo(m + 1, "yyyyMM"), DateUtils.monthsAgo(m + 3, "yyyyMM"))
  }

  private def allData = {
    BaseDao.getOrderProps(SQL().select("store_id,order_date,cigar_name,order_amount").where(s" order_date >= '${DateUtils.monthsAgo(15, "yyyy-MM-01")}' "))
      .map(a => ((a(0).toString, DateUtils.strToStr(a(1).toString, "yyyy-MM-dd", "yyyyMM"), a(2).toString), a(3).toString.toInt))
      .groupByKey()
      .map(t => (t._1, t._2.sum)).cache()
  }

  /**
   * 近12个月，每个月的订货品类数
   */
  def categoryPerMonth = {
    import Helper.storeIdAndOrderDateOrdering
    BaseDao.getOrderProps(SQL().select("store_id,order_date,cigar_name").where(s" order_date >= '${DateUtils.monthsAgo(12, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}' and order_amount > '0'"))
      .map(a => (a(0).toString, DateUtils.strToStr(a(1).toString, "yyyy-MM-dd", "yyyyMM"), a(2).toString))
      .distinct()
      .map(t => ((t._1, t._2), 1))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .sortBy(_._1)
      .map(t => (t._1._1, (t._1._2, t._2))).cache()
  }

  /**
   * 近12个月，每个月的订货次数
   */
  def orderNumberPerMonth = {
    import Helper.storeIdAndOrderDateOrdering
    selectLastMonthsData("store_id,order_date,order_id", 12)
      .map(a => (a(0).toString, DateUtils.strToStr(a(1).toString, "yyyy-MM-dd", "yyyyMM"), a(2).toString))
      .distinct()
      .map(t => ((t._1, t._2), 1))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .sortBy(_._1)
      .map(t => (t._1._1, (t._1._2, t._2)))
  }

  /**
   * 近12个月，每条均价
   */
  def perCigarPricePerMonth = {
    import Helper.storeIdAndOrderDateOrdering
    moneyAmountPerMonth.leftOuterJoin(orderAmountPerMonth)
      .map(t => (t._1, Utils.retainDecimal(t._2._1 / t._2._2.get, 2)))
      .sortBy(_._1)
      .map(t => (t._1._1, (t._1._2, t._2)))
  }

  /**
   * 近12个月,订货额的top5
   *
   */
  def payMoneyTop5PerMonth = {
    selectLastMonthsData("store_id,order_date,cigar_name,money_amount", 12)
      .map(a => ((a(0).toString, DateUtils.strToStr(a(1).toString, "yyyy-MM-dd", "yyyyMM"), a(2).toString), a(3).toString.toDouble))
      .groupByKey()
      .map(t => ((t._1._1, t._1._2), (Utils.retainDecimal(t._2.sum, 2), t._1._3)))
      .groupByKey()
      .map(t => (t._1, t._2.toList.sorted.reverse.take(5)))
      .map(t => (t._1._1, (t._1._2, t._2)))
      .groupByKey().collect()
  }

  /**
   * 获取当月活跃品类数
   */
  def getActiveCategoryInLastMonth = {
    BaseDao.getOrderProps(SQL().select("store_id,cigar_name,order_date,order_amount").where(s" order_date >= '${DateUtils.monthsAgo(3, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'"))
      .map(a => ((a(0).toString, a(1).toString), a(3).toString.toInt))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .filter(t => t._2 >= 9)
      .map(t => (t._1._1, 1))
      .groupByKey()
      .map(t => (t._1, t._2.sum)).persist()
  }

  /**
   * 单品毛利率 = （零售指导价-成本价）/指导价*100%
   * 按月计算
   * 返回：店铺id，年月，卷烟牌子，毛利率
   */
  def grossMarginPerMonthCategory = {
    val array = BaseDao.getOrderProps(SQL().select("store_id,order_date,money_amount,order_amount,retail_price,cigar_name").where("retail_price <> 'null'"))
      //零售指导价和成本价为0时,不参与计算,所以使用filter过滤掉
      .filter(a => a(2).toString != "0" && a(4).toString != "0")
      .map(a => ((a(0).toString, a(1).toString.substring(0, 7), a(5).toString), (a(2).toString.toDouble, a(3).toString.toInt * a(4).toString.toDouble)))
      .groupByKey()
      .map(t => (t._1, t._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))))
      .filter(t => t._2._1.toInt > 0 && t._2._2.toInt > 0)
      .map(t => ((t._1._1, t._1._2, Utils.retainDecimal(1 - t._2._1 / t._2._2, 3)), t._1._3))
      .collect()
      .sortWith((a, b) => a._1.toString() > b._1.toString())
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
    val array = BaseDao.getOrderProps(SQL().select("store_id,order_date,money_amount,order_amount,retail_price").where("retail_price <> 'null'"))
      //零售指导价和成本价为0时,不参与计算,所以使用filter过滤掉
      .filter(a => a(2).toString != "0" && a(4).toString != "0")
      .map(a => ((a(0).toString, a(1).toString.substring(0, 7)), (a(2).toString.toDouble, a(3).toString.toInt * a(4).toString.toDouble)))
      .groupByKey()
      .map(t => (t._1, t._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))))
      .map(t => (t._1._1, t._1._2, Utils.retainDecimal(1 - t._2._1 / t._2._2, 3))) //((33010220120807247A,2015-01),0.18)
      .collect().sortWith((a, b) => a.toString() > b.toString())
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
        .where(s" retail_price <> 'null' and order_date >= '${DateUtils.monthsAgo(12, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
    )
      //零售指导价和成本价为0时,不参与计算,所以使用filter过滤掉
      .filter(a => a(1).toString != "0" && a(3).toString != "0")
      .map(a => (a(0), (a(1).toString.toDouble, a(2).toString.toInt * a(3).toString.toDouble)))
      .groupByKey()
      .map(t => (t._1, t._2.reduce((a, b) => (a._1 + b._1, a._2 + b._2))))
      .map(t => (t._1.toString, Utils.retainDecimal(1 - t._2._1 / t._2._2, 3))).persist()
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
    BaseDao.getOrderProps(
      SQL().select("store_id,cigar_name,order_amount,retail_price")
        .where(s" retail_price <> 'null' and order_date >= '${DateUtils.monthsAgo(12, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
    ).map(a => (a(0).toString, a(1).toString, a(2).toString.toInt * a(3).toString.toDouble))
      .map(t => ((t._1, t._2), t._3))
      .groupByKey()
      .map(t => (t._1, t._2.sum))
      .map(t => ((t._1._1, t._2), t._1._2))
      .sortByKey(ascending = false).map(t => (t._1._1, (t._1._2, t._2))).collect() //((33010220120807247A,300.0),七匹狼(蓝))
  }

  private def getLast12MonthsSales = {
    BaseDao.getOrderProps(
      SQL().select("store_id,order_amount,retail_price").
        where(s"retail_price <> 'null' and order_date >= '${DateUtils.monthsAgo(12, "yyyy-MM-01")}' and order_date < '${DateUtils.monthsAgo(0, "yyyy-MM-01")}'")
    ).map(a => (a(0).toString, a(1).toString.toInt * a(2).toString.toDouble))
      .map(t => (t._1, t._2))
      .groupByKey()
      .map(t => (t._1, t._2.sum)) //((33010220120807247A,300.0),七匹狼(蓝))
  }

  /**
   * 销售额租金比
   * 暂时缺失数据，默认为0.6
   */
  def salesRentRatio = {
    Contexts.sparkContext.parallelize(storeIdArray).map((_, 0.6)).persist()
  }

  def storeIdArray = BaseDao.getOrderProps(SQL().select("store_id").where(s"order_date >= '${DateUtils.monthsAgo(12, "yyyy-MM-01")}'")).map(a => a(0).toString).distinct().collect()

  /**
   * 线下商圈指数
   * 暂时缺失数据，默认为0.8
   */
  def offlineShoppingDistrictIndex = {
    Contexts.sparkContext.parallelize(storeIdArray).map((_, 0.8D)).persist()
  }

  /**
   * 周订货量为0触发预警,
   * 返回（店铺ID,是否预警）
   * @return
   */
  def weekOrderAmountWarn = {
    val lastWeek = DateUtils.weeksAgo(1)
    val storesHaveOrderAmountInLastWeek = BaseDao.getOrderProps(SQL().select("store_id,order_amount")
      .where(s" order_date >= '${lastWeek._1}' and order_date <= '${lastWeek._2}'"))
      .map(a => (a(0).toString, a(1).toString.toInt))
      .groupByKey().map(t => (t._1, t._2.sum)).filter(t => t._2 != 0)
    loanStore.leftOuterJoin(storesHaveOrderAmountInLastWeek).map(t => if (t._2._2.isEmpty) (t._1, (t._2._2.getOrElse(0), true)) else (t._1, (t._2._2.getOrElse(0), false)))
  }

  def loanStore = {
    BaseDao.getLoanStoreProps(SQL().select("store_id")).map(a => (a(0).toString, ""))
  }

  /**
   * 预警:环比上四周周均下滑 30% 进货额。计算公式w5 /【(w1+w2+w3+w4)/4】 <= 0.7
   * 返回（店铺ID,是否预警）
   */
  def moneyAmountWarn = {
    val loanMoneyAmountRateWarnValue = 0.7
    val moneyAmountLastWeek = moneyAmountFor(DateUtils.weeksAgo(1))
    //因为计算出来是四周的总和，即(w1+w2+w3+w4)，所以要对进货额/4
    val moneyAmountFourWeekAvg = {
      moneyAmountFor((DateUtils.weeksAgo(5)._1, DateUtils.weeksAgo(2)._2)).map(t => (t._1, t._2 / 4))
    }
    val rate = moneyAmountLastWeek.leftOuterJoin(moneyAmountFourWeekAvg)
      .filter(_._2._2.isDefined)
      .map(t => (t._1, t._2._1 / t._2._2.get))
    loanStore.leftOuterJoin(rate)
      .map(t => (t._1, Utils.retainDecimal(t._2._2.getOrElse(0D), 3)))
      .map(t => if (t._2 > loanMoneyAmountRateWarnValue) (t._1, (t._2, false)) else (t._1, (t._2, true)))
  }

  def moneyAmountFor(dateRange: (String, String)) = {
    BaseDao.getOrderProps(SQL().select("store_id,money_amount,order_date")
      .where(s" order_date >= '${dateRange._1}' and order_date <= '${dateRange._2}'"))
      .map(a => (a(0).toString, a(1).toString.toDouble))
      .groupByKey().map(t => (t._1, Utils.retainDecimal(t._2.sum, 2)))
  }
}

object Helper {
  implicit val storeIdAndOrderDateOrdering: Ordering[(String, String)] = new Ordering[(String, String)] {
    override def compare(a: (String, String), b: (String, String)): Int =
      if (a._1 > b._1) 1
      else if (a._1 < b._1) -1
      else if (a._2 > b._2) -1
      else 1
  }
}