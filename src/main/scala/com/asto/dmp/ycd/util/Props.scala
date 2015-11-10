package com.asto.dmp.ycd.util

import java.io.FileInputStream
import java.util.Properties


object Props {
  private val prop = new Properties()

  /*
  在spark-submit中加入--driver-java-options -DPropPath=/home/hadoop/prop.properties的参数后，
  使用System.getProperty("PropPath")就能获取路径：/home/hadoop/prop.properties
  如果spark-submit中指定了prop.properties文件的路径，那么使用prop.properties中的属性，否则使用该类中定义的属性
  */
  private def getPropertyFile: String = {
    if (externalPropertiesExist) {
      System.getProperty("PropPath")
    } else {
      getClass().getResource("/").getPath() + "prop.properties"
    }
  }

  private def externalPropertiesExist: Boolean = Option(System.getProperty("PropPath")).isDefined

  prop.load(new FileInputStream(getPropertyFile))

  def get(propertyName: String): String = {
    new String(prop.getProperty(propertyName).getBytes("ISO-8859-1"), "utf-8")
  }

}