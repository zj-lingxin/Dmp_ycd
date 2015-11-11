package com.asto.dmp.ycd.base

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    val SPARK_UI_APP_NAME = "烟草贷"
    val CHINESE_NAME = "烟草贷"
    val HADOOP_DIR = "hdfs://appcluster/ycd/online"
    val LOG_WRAPPER = "##########"
    val YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd"
    val YEAR_MONTH_FORMAT = "yyyy-MM"
    var TODAY: String = _
    var STORE_ID: String = _
    var TIMESTAMP: Long = _
    var MQ_ENABLE: Boolean = _
    val ERROR_LOG: StringBuffer = new StringBuffer("")
  }

  /** 输入文件路径 **/
  object InputPath {
    val SEPARATOR = "\t"
    private val DIR = s"${App.HADOOP_DIR}/input"
    val ORDER_DETAILS = s"$DIR/tobacco_order_details/*"
    val TOBACCO_PRICE = s"$DIR/tobacco_price/*"
  }

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = "\t"
    private val DIR = s"${App.HADOOP_DIR}/output/${App.TODAY}/${App.STORE_ID}_${App.TIMESTAMP}"
    val CREDIT = s"$DIR/credit"
    val SCORE = s"$DIR/score"
    val GPA = s"$DIR/GPA"
    val FIELD = s"$DIR/field"
    val ACTIVE_CATEGORY = s"$DIR/activeCategory"
    val GROSS_MARGIN_PER_MONTH_CATEGORY = s"$DIR/grossMarginPerMonthCategory"
    val GROSS_MARGIN_PER_MONTH_ALL = s"$DIR/grossMarginPerMonthAll"
  }

  /** 表的模式 **/
  object Schema {
    //烟草订单详情：城市,店铺id,订单号,订货日期,卷烟名称,批发价,要货量(想要多少货),订货量(厂家给的货，也就是实际拿到的货),金额,生产厂家
    val ORDER_DETAILS = "store_id,order_id,order_date,cigar_name,wholesale_price,purchase_amount,order_amount,money_amount,producer_name"
    //卷烟名称,品牌系列,零售指导价(元/条),生产厂家
    val TOBACCO_PRICE = "cigar_name,cigar_brand,retail_price,producer_name"
    //ORDER_DETAILS和TOBACCO_PRICE关联的数据：城市, 店铺id, 订单号, 订货日期, 卷烟名称,（单条烟的）批发价|成本价,要货量(想要多少货),订货量(厂家给的货，也就是实际拿到的货),金额（要货量 * （单条烟的）批发价）,生产厂家
    val ORDER = "store_id,order_id,order_date,cigar_name,wholesale_price,purchase_amount,order_amount,money_amount,cigar_brand,retail_price,producer_name"
  }

}
