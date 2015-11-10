package com.asto.dmp.ycd.util

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
        logInfo(Utils.logWrapper(s"删除目录：$filePath"))
        HDFSFilesSystem.delete(filePath, true)
      }
    }
  }

  def saveAsTextFile[T <: Product](rdd: RDD[T], savePath: String, deleteExistingFiles: Boolean = true) = {
    if(deleteExistingFiles)
      deleteFilesInHDFS(savePath)
    rdd.map(_.productIterator.mkString(Constants.OutputPath.SEPARATOR)).coalesce(1).saveAsTextFile(savePath)
  }
}
