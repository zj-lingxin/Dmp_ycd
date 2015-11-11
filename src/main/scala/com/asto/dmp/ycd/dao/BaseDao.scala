package com.asto.dmp.ycd.dao

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.util.{Utils, DateUtils, BizUtils}
import org.apache.spark.rdd.RDD

object BaseDao extends Dao {

  private def getOrderDetailsProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.ORDER_DETAILS, Constants.Schema.ORDER_DETAILS, "tobacco_order_details", sql)

  private def getTobaccoPriceProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.TOBACCO_PRICE, Constants.Schema.TOBACCO_PRICE, "tobacco_prices", sql)

  private def orderAndPrice() = {
    val tobaccoPriceRDD = getTobaccoPriceProps(SQL().select("cigar_name,cigar_brand,retail_price,producer_name"))
      .map(a => (a(0).toString, (a(1).toString, a(2).toString, a(3).toString))).distinct()

    val orderDetailsRDD = getOrderDetailsProps(SQL().select("cigar_name,store_id,order_id,order_date,wholesale_price,purchase_amount,order_amount,money_amount").where(s"store_id = '${Constants.App.STORE_ID}'"))
      .map(a => (a(0).toString, (a(1).toString, a(2).toString, a(3).toString, a(4).toString, a(5).toString, a(6).toString, a(7).toString)))

    tobaccoPriceRDD.leftOuterJoin(orderDetailsRDD).filter(_._2._2.isDefined).map(t => (t._2._2.get._1, t._2._2.get._2, t._2._2.get._3, t._1, t._2._2.get._4, t._2._2.get._5, t._2._2.get._6, t._2._2.get._7, t._2._1._1, t._2._1._2, t._2._1._3))
  }

  def getOrderProps(sql: SQL = new SQL()) = getProps(orderAndPrice(), Constants.Schema.ORDER, "full_fields_order", sql)
}
