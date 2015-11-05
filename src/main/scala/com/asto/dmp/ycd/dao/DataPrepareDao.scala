package com.asto.dmp.ycd.dao

import com.asto.dmp.ycd.base.SQL

object DataPrepareDao {
  def fullFieldsOrder() = {
    val tobaccoPriceRDD = BizDao.getTobaccoPriceProps(SQL().select("cigarette_name,cigarette_brand,retail_price,manufacturers"))
      .map(a => (a(0).toString, (a(1), a(2), a(3))))

    val orderDetailsRDD = BizDao.getOrderDetailsProps(SQL().select("cigarette_name,city,license_no,order_id,order_date,the_cost,need_goods_amount,order_amount,pay_money"))
      .map(a => (a(0).toString, (a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8))))
    tobaccoPriceRDD.leftOuterJoin(orderDetailsRDD).filter(_._2._2.isDefined).map(t => (t._2._2.get._1, t._2._2.get._2, t._2._2.get._3, t._2._2.get._4,t._1,t._2._2.get._5,t._2._2.get._6,t._2._2.get._7,t._2._2.get._8,t._2._1._1,t._2._1._2,t._2._1._3))
  }
}
