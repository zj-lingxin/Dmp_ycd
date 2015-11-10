package com.asto.dmp.ycd.base

import com.asto.dmp.ycd.util.Utils
import org.apache.spark.Logging
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StringType, StructField, StructType}

trait Dao extends Logging {
  protected def getProps(inputFilePath: String, schema: String, tempTableName: String, sqlObj: SQL, separator: String = Constants.InputPath.SEPARATOR) = {
    registerTempTableIfNotExist(inputFilePath, schema, tempTableName, separator)

    var _sql = s"SELECT ${sqlObj.select} FROM $tempTableName"

    if (Option(sqlObj.where).isDefined)
      _sql += s" WHERE ${sqlObj.where}"

    if (Option(sqlObj.orderBy).isDefined) {
      //使用OrderBy的时候，需要将spark.sql.shuffle.partitions设小
      Contexts.sqlContext.sql(s"SET spark.sql.shuffle.partitions=10")
      logInfo(Utils.logWrapper("order by 操作需要设置: SET spark.sql.shuffle.partitions=200 "))
      _sql += s" ORDER BY  ${sqlObj.orderBy} "
    }

    //暂时不支持 group by，请使用相关的Transforamtion操作

    if (Option(sqlObj.limit).isDefined)
      _sql += s" LIMIT ${sqlObj.limit}"

    logInfo(Utils.logWrapper(s"执行Sql:${_sql}"))
    val rdd = Contexts.sqlContext.sql(_sql).map(a => a.toSeq.toArray)

    if (Option(sqlObj.orderBy).isDefined) {
      //order by操作完成后设回默认值200
      logInfo(Utils.logWrapper("order by 操作完成,设回默认值: SET spark.sql.shuffle.partitions=200"))
      Contexts.sqlContext.sql("SET spark.sql.shuffle.partitions=200")
    }
    rdd
  }

  private def registerTempTableIfNotExist(inputFilePath: String, schema: String, tempTableName: String, separator: String) {
    //如果临时表未注册，就进行注册
    if (!Contexts.sqlContext.tableNames().contains(tempTableName)) {
      val fieldsNum = schema.split(",").length
      val tmpRDD = Contexts.sparkContext.textFile(inputFilePath)
        .map(_.split(separator)).cache()
      tmpRDD.map(t =>(t.length,1)).countByValue()
      logInfo(Utils.logWrapper(s"schema的字段是$fieldsNum,解析后的字段个数与行数的关系是:${tmpRDD.map(_.length).countByValue()}"))

      val rowRDD = tmpRDD.filter(x => x.length == fieldsNum)
        .map(fields => for (field <- fields) yield field.trim)
        .map(fields => Row(fields: _*))
      Contexts.sqlContext.createDataFrame(rowRDD, simpleSchema(schema)).registerTempTable(tempTableName)
      logInfo(Utils.logWrapper(s"注册临时表：$tempTableName"))
    }
  }

  private def simpleSchema(schemaStr: String): StructType = {
    StructType(schemaStr.toLowerCase.split(",").map(fieldName => StructField(fieldName.trim, StringType, nullable = true)))
  }

}
