package com.asto.dmp.ycd.base

import com.asto.dmp.ycd.util.DateUtils

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    val SPARK_UI_APP_NAME = "烟草贷"
    val CHINESE_NAME = "烟草贷"
    val HADOOP_DIR = "hdfs://appcluster/ycd/"
    val LOG_WRAPPER = "##########"
    val YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd"
    val YEAR_MONTH_FORMAT = "yyyy-MM"
    val TODAY = DateUtils.getStrDate("yyyyMM/dd")
  }

  /** 输入文件路径 **/
  object InputPath {
    val SEPARATOR = "\t"
    private val DIR = s"${App.HADOOP_DIR}/input"
    val ORDER_INFO = s"$DIR/tobacco_order_info/*"
    val ORDER_DETAILS = s"$DIR/tobacco_order_details/*"
    val TOBACCO_PRICE = s"$DIR/tobacco_price/*"
    //整合了ORDER_DETAILS和TOBACCO_PRICE的信息
    val FULL_FIELDS_ORDER = s"$DIR/full_fields_order"
  }

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = "\t"
    private val DIR = s"${App.HADOOP_DIR}/output"

    val CREDIT = s"$DIR/text/${App.TODAY}/credit"
    val SCORE = s"$DIR/text/${App.TODAY}/score"
    val GPA = s"$DIR/text/${App.TODAY}/GPA"
    val FIELD = s"$DIR/text/${App.TODAY}/field"
    val ACTIVE_CATEGORY = s"$DIR/text/${App.TODAY}/activeCategory"
    val GROSS_MARGIN_PER_MONTH_CATEGORY = s"$DIR/text/${App.TODAY}/grossMarginPerMonthCategory"
    val GROSS_MARGIN_PER_MONTH_ALL = s"$DIR/text/${App.TODAY}/grossMarginPerMonthAll"
  }

  /** 表的模式 **/
  object Schema {
    //烟草订单详情：城市,许可证号,订单号,订货日期,卷烟名称,批发价,要货量(想要多少货),订货量(厂家给的货，也就是实际拿到的货),金额,生产厂家(与TOBACCO_PRICE中的冗余)
    val ORDER_DETAILS = "city,license_no,order_id,order_date,cigarette_name,the_cost,need_goods_amount,order_amount,pay_money,manufacturers"
    //卷烟名称,品牌系列,零售指导价(元/条),生产厂家
    val TOBACCO_PRICE = "cigarette_name,cigarette_brand,retail_price,manufacturers"
    //烟草订单详情：           城市,   许可证号,   订单号,   订货日期,       卷烟名称,（单条烟的）批发价|成本价,要货量(想要多少货),订货量(厂家给的货，也就是实际拿到的货),金额（要货量 * （单条烟的）批发价）,生产厂家(与TOBACCO_PRICE中的冗余)
    val FULL_FIELDS_ORDER = "city,license_no,order_id,order_date,cigarette_name,the_cost,need_goods_amount,order_amount,pay_money,cigarette_brand,retail_price,manufacturers"
  }

  /** 邮件发送功能相关常量 **/
  object Mail {
    //以下参数与prop.properties中的参数一致。
    val TO = "lingx@asto-inc.com"
    val FROM = "dmp_notice@asto-inc.com"
    val PASSWORD = "astodmp2015"
    val SMTPHOST = "smtp.qq.com"
    val SUBJECT = s"“${App.CHINESE_NAME}”项目出异常了！"
    val CC = ""
    val BCC = ""
    //ENABLE为false时，不启用邮件发送功能，为true是可以使用
    val ENABLE = "false"

    //以下参数prop.properties中没有， MAIL_CREDIT_SUBJECT是授信规则模型出问题时邮件的主题
    val SCORE_SUBJECT = s"${App.CHINESE_NAME}-评分规则结果集写入失败，请尽快查明原因！"
    val FIELDS_CALCULATION_SUBJECT = s"${App.CHINESE_NAME}-字段计算的结果写入失败，请尽快查明原因！"
    val CREDIT_SUBJECT = s"${App.CHINESE_NAME}-授信规则结果集写入失败，请尽快查明原因！"
    val DATA_PREPARE_SUBJECT = s"${App.CHINESE_NAME}-数据准备结果集写入失败，请尽快查明原因！"
  }
}
