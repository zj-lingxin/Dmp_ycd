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

  def orderAndPrice () = {
    val tobaccoPriceRDD = getTobaccoPriceProps(SQL().select("cigar_name,cigar_brand,retail_price,producer_name,wholesale_price,area_code"))
      .map(a => ((a(0).toString,a(5).toString), (a(1).toString, a(2).toString, a(3).toString,a(4).toString))).distinct()
    val orderDetailsRDD = getOrderDetailsProps(orderSql)
      .map(a => ((a(0).toString,a(8).toString), (a(1).toString, a(2).toString, a(3).toString, a(4).toString, a(5).toString, a(6).toString, a(7).toString)))
    //TobaccoPrice和OrderDetails中都有批发价，采用TobaccoPrice中的批发价
    var resultRDD = tobaccoPriceRDD.rightOuterJoin(orderDetailsRDD).map(t => (t._1,(t._2._1.getOrElse((null,null,null,null)),t._2._2))).map(t => (t._2._2._1, t._2._2._2, t._2._2._3, t._1._1, t._2._2._4, t._2._1._4, t._2._2._6, t._2._2._7, t._2._1._1, t._2._1._2, t._2._1._3, t._1._2))

    //如果是离线模型，过滤掉状态为“作废”的订单
    //在线模型对“作废”的订单的过滤已经在Sqoop的mysql语句中处理了。
    //由于离线模型的数据数量太多，大数据量加上复杂的表关联时，sqoop导入数据时会卡死。所以对“作废”订单的过滤只能在这里过滤了
    /*if(!Constants.App.IS_ONLINE) {*/
    val invalidOrderIds =  getInvalidOrderId().filter(_.size == 1).map(_(0).toString).collect()
    resultRDD = resultRDD.filter(t => !invalidOrderIds.contains(t._2)) //过滤出有效的数据
   /* }*/
    resultRDD
  }

  def getOrderProps(sql: SQL = new SQL()) = getProps(orderAndPrice(), Constants.Schema.ORDER, "full_fields_order", sql)

  def getLoanStoreProps(sql: SQL = new SQL()) = getProps(Constants.InputPath.LOAN_STORE, Constants.Schema.LOAN_STORE, "loan_store", sql)
}
