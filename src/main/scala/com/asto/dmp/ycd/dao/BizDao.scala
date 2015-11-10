package com.asto.dmp.ycd.dao

import com.asto.dmp.ycd.base._
import com.asto.dmp.ycd.util.{Utils, DateUtils, BizUtils}

object BizDao extends Dao {

  def getOrderDetailsProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.ORDER_DETAILS, Constants.Schema.ORDER_DETAILS, "tobacco_order_details", sql)

  def getTobaccoPriceProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.TOBACCO_PRICE, Constants.Schema.TOBACCO_PRICE, "tobacco_prices", sql)

  def getFullFieldsOrderProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.TEMP_ORDER, Constants.Schema.FULL_FIELDS_ORDER, "full_fields_order", sql)

}
