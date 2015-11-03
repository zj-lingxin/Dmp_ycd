package com.asto.dmp.ycd.dao

import com.asto.dmp.ycd.base._

object BizDao extends Dao {

//  def getOrderInfoProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.ORDER_INFO, Constants.Schema.ORDER_INFO, "tobacco_order_info", sql)

  def getOrderDetailsProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.ORDER_DETAILS, Constants.Schema.ORDER_DETAILS, "tobacco_order_details", sql)

  def getTobaccoPriceProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.TOBACCO_PRICE, Constants.Schema.TOBACCO_PRICE, "tobacco_prices", sql)

  def getFullFieldsOrderProps(sql: SQL = new SQL()) = getProps(Constants.OutputPath.FULL_FIELDS_ORDER, Constants.Schema.FULL_FIELDS_ORDER, "full_fields_order", sql, Constants.OutputPath.SEPARATOR)

}
