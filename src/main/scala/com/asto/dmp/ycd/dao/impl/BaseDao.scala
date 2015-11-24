package com.asto.dmp.ycd.dao.impl

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.dao.{Dao, SQL}

object BaseDao extends Dao {
  private def orderDetailsPath = {
    if(Option(Constants.App.STORE_ID).isDefined) Constants.InputPath.ORDER_DETAILS_ONLINE
    else Constants.InputPath.ORDER_DETAILS_OFFLINE
  }

  private def getOrderDetailsProps(sql: SQL = new SQL()) = {
    getProps(orderDetailsPath, Constants.Schema.ORDER_DETAILS, "tobacco_order_details", sql)
  }

  private def getTobaccoPriceProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.TOBACCO_PRICE, Constants.Schema.TOBACCO_PRICE, "tobacco_prices", sql)

  private def orderSql = {
    val sql = SQL().select("cigar_name,store_id,order_id,order_date,wholesale_price,purchase_amount,order_amount,money_amount,area_code")
    if(Option(Constants.App.STORE_ID).isDefined) sql.where(s"order_date <> 'null' and store_id = '${Constants.App.STORE_ID}'")
    else sql.where(s"order_date <> 'null'")
    sql
  }

  def orderAndPrice () = {
    val tobaccoPriceRDD = getTobaccoPriceProps(SQL().select("cigar_name,cigar_brand,retail_price,producer_name,wholesale_price,area_code"))
      .map(a => ((a(0).toString,a(5).toString), (a(1).toString, a(2).toString, a(3).toString,a(4).toString))).distinct()
    val orderDetailsRDD = getOrderDetailsProps(orderSql)
      .map(a => ((a(0).toString,a(8).toString), (a(1).toString, a(2).toString, a(3).toString, a(4).toString, a(5).toString, a(6).toString, a(7).toString)))
    //TobaccoPrice和OrderDetails中都有批发价，采用TobaccoPrice中的批发价
    tobaccoPriceRDD.rightOuterJoin(orderDetailsRDD).map(t => (t._1,(t._2._1.getOrElse((null,null,null,null)),t._2._2))).map(t => (t._2._2._1, t._2._2._2, t._2._2._3, t._1._1, t._2._2._4, t._2._1._4, t._2._2._6, t._2._2._7, t._2._1._1, t._2._1._2, t._2._1._3, t._1._2))
  }

  def getOrderProps(sql: SQL = new SQL()) = getProps(orderAndPrice(), Constants.Schema.ORDER, "full_fields_order", sql)

  def getLoanStoreProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.LOAN_STORE, Constants.Schema.LOAN_STORE, "loan_store", sql)
}
