package com.asto.dmp.ycd.dao

import com.asto.dmp.ycd.base._

object BizDao extends Dao {

  def getOrderInfoProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.ORDER_INFO, Constants.Schema.ORDER_INFO, "tobacco_order_info", sql)

  def getOrderDetailsProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.ORDER_DETAILS, Constants.Schema.ORDER_DETAILS, "tobacco_order_details", sql)

}
