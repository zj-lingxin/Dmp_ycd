package com.asto.dmp.ycd.base

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    val NAME = "烟草贷"
    val LOG_WRAPPER = "##########"
    val YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd"
    val YEAR_MONTH_FORMAT = "yyyy-MM"
    val DIR = s"${Hadoop.DEFAULT_FS}/ycd"
    var TODAY: String = _
    var STORE_ID: String = _
    var IS_ONLINE: Boolean = true
    var RUN_CODE: String = _
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
    //在线目录
    private val ONLINE_DIR = s"${App.DIR}/input/online/${App.TODAY}/${App.STORE_ID}_${App.TIMESTAMP}"
    //离线目录
    private val OFFLINE_DIR = s"${App.DIR}/input/offline/${App.TODAY}"

    private val dirAndFileName = (fileName: String) => if (App.IS_ONLINE) s"$ONLINE_DIR/$fileName/*" else s"$OFFLINE_DIR/${fileName}_${App.TIMESTAMP}/*"

    val ORDER_DETAILS = dirAndFileName("tobacco_order_details")

    //作废的订单Id获取路径,离线批量模型需要用到。在线模型对“作废”的订单的过滤已经在Sqoop的mysql语句中过滤了。
    //由于离线模型的数据数量太多，大数据量加上复杂的表关联时，sqoop导入数据时会卡死。所以对“作废”订单的过滤只能使用spark过滤了
    //val INVALID_ORDER_ID = s"$OFFLINE_DIR/invalid_order_id_${App.TIMESTAMP}/*"

    val INVALID_ORDER_ID = dirAndFileName("invalid_order_id")
    //val LOAN_STORE = s"$OFFLINE_DIR/loan_store_${App.TIMESTAMP}/*"
    val LOAN_STORE = dirAndFileName("loan_store")

    val STORE_ID_CALC_MONTHS = dirAndFileName("store_id_calc_months")
  }
  

  /** 输出文件路径 **/
  object OutputPath {
    private val dirAndFileName = (fileName: String) => if (App.IS_ONLINE) s"$ONLINE_DIR/$fileName" else s"$OFFLINE_DIR/$fileName"
    val SEPARATOR = "\t"
    private val ONLINE_DIR = s"${App.DIR}/output/online/${App.TODAY}/${App.STORE_ID}_${App.TIMESTAMP}"
    private val OFFLINE_DIR = s"${App.DIR}/output/offline/${App.TODAY}/${App.TIMESTAMP}"
    val MONEY_AMOUNT_PATH = dirAndFileName("moneyAmount")
    val ORDER_AMOUNT_PATH = dirAndFileName("orderAmount")
    val CATEGORY_AMOUNT_PATH = dirAndFileName("category")
    val ORDER_NUMBER_PATH = dirAndFileName("orderNumber")
    val PER_CIGAR_PRICE_PATH = dirAndFileName("perCigarPrice")
    val ACTIVE_CATEGORY_PATH = dirAndFileName("activeCategory")
    val MONEY_AMOUNT_TOP5_PATH = dirAndFileName("moneyAmountTop5PerMonth")
    val INDEXES_PATH = dirAndFileName("indexes")
    val SCORE_PATH = dirAndFileName("scores")
    val CREDIT_AMOUNT_PATH = dirAndFileName("creditAmount")
    val LOAN_WARN_PATH = s"$OFFLINE_DIR/loanWarn"
  }

  /** 表的模式 **/
  object Schema {
    //烟草订单详情：店铺id,订单号,订货日期,卷烟名称,批发价,要货量(想要多少货),订货量(厂家给的货，也就是实际拿到的货),金额,生产厂家
    val ORDER_DETAILS = "store_id,order_id,order_date,cigar_name,wholesale_price,purchase_amount,order_amount,money_amount,producer_name,area_code"
    //卷烟名称,品牌系列,零售指导价(元/条),生产厂家
    val TOBACCO_PRICE = "cigar_name,cigar_brand,retail_price,producer_name,wholesale_price,area_code"
    //ORDER_DETAILS和TOBACCO_PRICE关联的数据： 店铺id, 订单号, 订货日期, 卷烟名称,（单条烟的）批发价|成本价,要货量(想要多少货),订货量(厂家给的货，也就是实际拿到的货),金额（订货量 * （单条烟的）批发价）,品牌系列,零售指导价(元/条),生产厂家,地区编码
    val ORDER = "store_id,order_id,order_date,cigar_name,wholesale_price,purchase_amount,order_amount,money_amount,cigar_brand,retail_price,producer_name,area_code"
    //进入贷后的店铺id
    val LOAN_STORE = "store_id"
    //无效的订单ID(这里指datag_yancao_historyOrderData表中status字段为"作废"的订单ID)
    val INVALID_ORDER_ID = "invalid_order_id"
    //每个店铺需要计算几个月份的数据。
    val STORE_ID_CALC_MONTHS = "store_id,calc_months"
  }
}
