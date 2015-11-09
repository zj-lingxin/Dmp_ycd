package com.asto.dmp.ycd.util

import java.io.FileInputStream
import java.util.Properties
import com.asto.dmp.ycd.base.Constants
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.spark.Logging
import org.apache.spark.rdd.RDD

/**
 * 文件相关的工具类
 */
object FileUtils extends Logging {
  def deleteFilesInHDFS(paths: String*) = {
    paths.foreach { path =>
      val filePath = new Path(path)
      val HDFSFilesSystem = filePath.getFileSystem(new Configuration())
      if (HDFSFilesSystem.exists(filePath)) {
        logInfo(Utils.wrapLog(s"删除目录：$filePath"))
        HDFSFilesSystem.delete(filePath, true)
      }
    }
  }

  def saveAsTextFile[T <: Product](rdd: RDD[T], savePath: String, deleteExistingFiles: Boolean = true) = {
    if(deleteExistingFiles)
      deleteFilesInHDFS(savePath)
    rdd.map(_.productIterator.mkString(Constants.OutputPath.SEPARATOR)).coalesce(1).saveAsTextFile(savePath)
  }

  //MQ相关方法
  val prop = new Properties()
  val propPath = System.getProperty("PropPath")
  val hasPropPath = if(propPath == null) false else true
  def getPropByKey(propertyKey: String): String = {
    if (hasPropPath) {
      prop.load(new FileInputStream(propPath))
      new String(prop.getProperty(propertyKey).getBytes("ISO-8859-1"),"utf-8")
    } else
      prop.getProperty(propertyKey)
  }
}
