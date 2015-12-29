package com.asto.dmp.ycd.dao.impl

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.dao.{Dao, SQL}

object BaseDao extends Dao {

  private def getOrderDetailsProps(sql: SQL = new SQL()) = {
    getProps(Constants.InputPath.ORDER_DETAILS, Constants.Schema.ORDER_DETAILS, "tobacco_order_details", sql)
  }

  private def getTobaccoPriceProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.TOBACCO_PRICE, Constants.Schema.TOBACCO_PRICE, "tobacco_prices", sql)

  private def orderSql = {
    val sql = SQL().select("cigar_name,store_id,order_id,order_date,wholesale_price,purchase_amount,order_amount,money_amount,area_code")
    if(Option(Constants.App.STORE_ID).isDefined) sql.where(s"order_date <> 'null' and store_id = '${Constants.App.STORE_ID}'")
    else sql.where(s"order_date <> 'null'")
    sql
  }

  private def getInvalidOrderId(sql: SQL = new SQL()) = getProps(Constants.InputPath.INVALID_ORDER_ID, Constants.Schema.INVALID_ORDER_ID, "invalid_order_id", sql)

  /**
   * 最后的相应字段(当价格表不存在时,会影响毛利率和集中度的计算)
   * store_id,                        order_id,    order_date,cigar_name,    wholesale_price,purchase_amount,order_amount,money_amount,cigar_brand,retail_price,producer_name,area_code
   * 69c8fff32f7d47c7bbc380c538d7ed0a,131102860971,2013-11-04,黄金叶(软大金圆),178.0,          null,           1,           178.0,       null,       null,        null,         320200)
   * @return
   */
  def orderAndPrice () = {
    val tobaccoPriceRDD = getTobaccoPriceProps(SQL().select("cigar_name,cigar_brand,retail_price,producer_name,wholesale_price,area_code"))
      .map(a => ((a(0).toString,a(5).toString), (a(1).toString, a(2).toString, a(3).toString,a(4).toString))).distinct()
    val orderDetailsRDD = getOrderDetailsProps(orderSql)
      .map(a => ((a(0).toString,a(8).toString), (a(1).toString, a(2).toString, a(3).toString, a(4).toString, a(5).toString, a(6).toString, a(7).toString)))

    val invalidOrderIds = getInvalidOrderId().filter(_.size == 1).map(_(0).toString).collect()
    //TobaccoPrice和OrderDetails中都有批发价，采用TobaccoPrice中的批发价
    tobaccoPriceRDD.rightOuterJoin(orderDetailsRDD)
      .map(t => (t._1,(t._2._1.getOrElse((null,null,null,null)),t._2._2)))
      .map(t => (t._2._2._1, t._2._2._2, t._2._2._3, t._1._1, t._2._2._4, t._2._1._4, t._2._2._6, t._2._2._7, t._2._1._1, t._2._1._2, t._2._1._3, t._1._2))
      .filter(t => !invalidOrderIds.contains(t._2)) //过滤出有效的数据
  }

  def getOrderProps(sql: SQL = new SQL()) = getProps(orderAndPrice(), Constants.Schema.ORDER, "full_fields_order", sql)

  def getLoanStoreProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.LOAN_STORE, Constants.Schema.LOAN_STORE, "loan_store", sql)

  def getStoreIdCalcMonthsProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.STORE_ID_CALC_MONTHS, Constants.Schema.STORE_ID_CALC_MONTHS, "store_id_calc_months", sql)

  def getShopYearRentProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.SHOP_YEAR_RENT, Constants.Schema.SHOP_YEAR_RENT, "shop_year_rent", sql)

}
