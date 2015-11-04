package com.asto.dmp.ycd.util

import java.util.Calendar

object BizUtils {
  /**
   * 输入一个时间，计算这个时间距离当前的时间有多少个月份
   */
  def monthsNumsFrom(date: String, format: String) = {
    val now = Calendar.getInstance()
    val theEarliestDate = DateUtils.strToCalendar(date, format)
    (now.get(Calendar.YEAR) * 12 + now.get(Calendar.MONTH)) -
      (theEarliestDate.get(Calendar.YEAR) * 12 + theEarliestDate.get(Calendar.MONTH))
  }

  /**
   * 保留两位小数
   * @param number
   * @return
   */
  def retainTwoDecimal(number: Double):Double = {
    f"${number}%1.2f".toDouble
  }
}
