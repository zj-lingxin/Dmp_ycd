package com.asto.dmp.ycd.base

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    val NAME = "烟草贷"
    val LOG_WRAPPER = "##########"
    val YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd"
    val YEAR_MONTH_FORMAT = "yyyy-MM"
    val DIR = s"${Hadoop.DEFAULT_FS}/ycd/"
    var TODAY: String = _
    var STORE_ID: String = _
    var TIMESTAMP: Long = _
    val ERROR_LOG: StringBuffer = new StringBuffer("")
    var MESSAGES: StringBuffer = new StringBuffer("")
  }
  
  object Hadoop {
    val JOBTRACKER_ADDRESS = "appcluster"
    val DEFAULT_FS = s"hdfs://$JOBTRACKER_ADDRESS"
  }
  /** 输入文件路径 **/
  object InputPath {
    val SEPARATOR = "\t"
    //共用
    val TOBACCO_PRICE = s"${App.DIR}/input/tobacco_price/*"
    //在线
    private val ONLINE_DIR = s"${App.DIR}/input/online/${App.TODAY}/${App.STORE_ID}_${App.TIMESTAMP}"
    val ORDER_DETAILS_ONLINE = s"$ONLINE_DIR/tobacco_order_details/*"
    //离线
    private val OFFLINE_DIR = s"${App.DIR}/input/offline/${App.TODAY}"
    val ORDER_DETAILS_OFFLINE = s"$OFFLINE_DIR/tobacco_order_details/*"
  }
  

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = "\t"
    private val ONLINE_DIR = s"${App.DIR}/output/online/${App.TODAY}/${App.STORE_ID}_${App.TIMESTAMP}"
    val MESSAGES_PATH_ONLINE = s"$ONLINE_DIR/messages"
  }

  /** 表的模式 **/
  object Schema {
    //烟草订单详情：店铺id,订单号,订货日期,卷烟名称,批发价,要货量(想要多少货),订货量(厂家给的货，也就是实际拿到的货),金额,生产厂家
    val ORDER_DETAILS = "store_id,order_id,order_date,cigar_name,wholesale_price,purchase_amount,order_amount,money_amount,producer_name"
    //卷烟名称,品牌系列,零售指导价(元/条),生产厂家
    val TOBACCO_PRICE = "cigar_name,cigar_brand,retail_price,producer_name"
    //ORDER_DETAILS和TOBACCO_PRICE关联的数据： 店铺id, 订单号, 订货日期, 卷烟名称,（单条烟的）批发价|成本价,要货量(想要多少货),订货量(厂家给的货，也就是实际拿到的货),金额（要货量 * （单条烟的）批发价）,品牌系列,零售指导价(元/条),生产厂家
    val ORDER = "store_id,order_id,order_date,cigar_name,wholesale_price,purchase_amount,order_amount,money_amount,cigar_brand,retail_price,producer_name"
  }
}
