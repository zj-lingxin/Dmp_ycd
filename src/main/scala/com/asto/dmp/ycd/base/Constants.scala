package com.asto.dmp.ycd.base

import com.asto.dmp.ycd.util.DateUtils

object Constants {

  /** App中的常量与每个项目相关 **/
  object App {
    //spark UI界面显示的名称
    val SPARK_UI_APP_NAME = "烟草贷"
    //项目的中文名称
    val CHINESE_NAME = "烟草贷"
    //项目数据存放的Hadoop的目录
    val HADOOP_DIR = "hdfs://appcluster/ycd/"

    val LOG_WRAPPER = "######################"

    val YEAR_MONTH_DAY_FORMAT = "yyyy-MM-dd"

    val YEAR_MONTH_FORMAT = "yyyy-MM"
  }

  /** 输入文件路径 **/
  object InputPath {
    //hdfs中表的字段的分隔符
    val SEPARATOR = "\t"
    private val DIR = s"${App.HADOOP_DIR}/input"
    val ORDER_INFO = s"$DIR/tobacco_order_info/*"
    val ORDER_DETAILS = s"$DIR/tobacco_order_details/*"
    val TOBACCO_PRICE = s"$DIR/tobacco_price/*"
  }

  /** 输出文件路径 **/
  object OutputPath {
    val SEPARATOR = "\t"
    private val TODAY = DateUtils.getStrDate("yyyyMM/dd")
    private val DIR = s"${App.HADOOP_DIR}/output"

    //整合了ORDER_DETAILS和TOBACCO_PRICE的信息
    val FULL_FIELDS_ORDER = s"$DIR/text/$TODAY/full_fields_order"

    //反欺诈结果路径
    val ANTI_FRAUD_TEXT = s"$DIR/text/$TODAY/fraud"
    //得分结果路径
    val SCORE_TEXT = s"$DIR/text/$TODAY/score"
    //授信结果路径
    val CREDIT_TEXT = s"$DIR/text/$TODAY/credit"
    //准入结果路径
    val ACCESS_TEXT = s"$DIR/text/$TODAY/access"
    //贷后预警结果路径
    val LOAN_WARNING_TEXT = s"$DIR/text/$TODAY/warning"
  }

  /** 表的模式 **/
  object Schema {
    //烟草订单信息：客户名称,许可证号,结算方式,订单号,状态,订货日期,品种数,总要货,总销售,进货额(成本),支付方式,城市,城市Id
    //val ORDER_INFO = "customer_name,license_no,billing_methods,order_id,status,order_date,number_of_species,total_need_goods_amount,total_sales,total_pay_money,pay_by,city,city_id"*/

    //烟草订单详情：城市,许可证号,订单号,订货日期,卷烟名称,批发价,要货量,订货量,金额,生产厂家(与TOBACCO_PRICE中的冗余)
    val ORDER_DETAILS = "city,license_no,order_id,order_date,cigarette_name,wholesale_price,need_goods_amount,order_amount,pay_money,manufacturers"
    //卷烟名称,品牌系列,零售指导价(元/条),生产厂家
    val TOBACCO_PRICE = "cigarette_name,cigarette_brand,retail_price,manufacturers"

    //烟草订单详情：城市,许可证号,订单号,订货日期,卷烟名称,批发价,要货量,订货量,金额,生产厂家(与TOBACCO_PRICE中的冗余)
    val FULL_FIELDS_ORDER = "city,license_no,order_id,order_date,cigarette_name,wholesale_price,need_goods_amount,order_amount,pay_money,manufacturers,cigarette_brand,retail_price,manufacturers"
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
    val CREDIT_SUBJECT = s"${App.CHINESE_NAME}-授信规则结果集写入失败，请尽快查明原因！"
    val ACCESS_SUBJECT = s"${App.CHINESE_NAME}-准入规则结果集写入失败，请尽快查明原因！"
    val ANTI_FRAUD_SUBJECT = s"${App.CHINESE_NAME}-反欺诈规则结果集写入失败，请尽快查明原因！"
    val LOAN_WARNING_SUBJECT = s"${App.CHINESE_NAME}-贷后预警规则结果集写入失败，请尽快查明原因！"
    val DATA_PREPARE_SUBJECT = s"${App.CHINESE_NAME}-数据准备结果集写入失败，请尽快查明原因！"
  }

}
