package hadoop

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, FileUtil, Path}

import scala.io.Source

object HdfsHelper {

  def main(args: Array[String]): Unit = {
    val hdfsMaster = "hdfs://<dfs.nameservices>:8020"
    val fs = HdfsHelper.getFileSystemInstance(hdfsMaster)

    val csvPath = ""
    val tagName = "test-tag"
    HdfsHelper.merge(fs, s"$csvPath/$tagName",
                     fs, s"$csvPath/churn-$tagName.csv",
         true, new Configuration(), null)

  }

  def getFileSystemInstance(masterUrl: String): FileSystem = {
    FileSystem.get(new URI(masterUrl), new Configuration())
  }

  def copyFromLocalFile(fs: FileSystem, src: String, dst: String): Unit = {
    val srcPath = new Path(src)
    val dstPath = new Path(dst)

    try {
      fs.copyFromLocalFile(srcPath, dstPath);
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      fs.close()
    }
  }


  def copyToLocalFile(fs: FileSystem, src: String, dst: String): Unit = {

    val srcPath = new Path(src)
    val dstPath = new Path(dst)

    try {
      fs.copyToLocalFile(srcPath, dstPath);
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      fs.close()
    }
  }

  def writeToHdfs(fs: FileSystem, file: String, data: Array[Byte]) = {

    val path = new Path(file)

    try {
      val os = fs.create(path)
      os.write(data)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      fs.close()
    }

  }


  def readFromHdfs(fs: FileSystem, file: String) = {

    try {
      val path = new Path(file)
      val stream = fs.open(path).getWrappedStream
      Source.fromInputStream(stream).mkString
    } catch {
      case e: IOException =>
        e.printStackTrace()
        ""
    } finally {
      fs.close()
    }
  }


  def rename(fs: FileSystem, src: String, dst: String) = {

    val srcPath = new Path(src)
    val dstPath = new Path(dst)

    try {
      fs.globStatus(srcPath).foreach { f =>
        fs.rename(f.getPath, dstPath);
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
        ""
    } finally {
      fs.close()
    }
  }

  /**
   *
   * @param fs
   * @param srcFilePattern eg: /user/sqoop/part*
   * @param dstFilePrefix
   */
  def renameBulk(fs: FileSystem, srcFilePattern: String, dstFilePrefix: String, dstFileSuffix: String) = {

    val srcPath = new Path(srcFilePattern)

    try {
      fs.globStatus(srcPath).foreach { file =>
        val dstPath = new Path(dstFilePrefix + System.currentTimeMillis() + dstFileSuffix)
        fs.rename(file.getPath, dstPath);
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      fs.close()
    }
  }

  def close(fs: FileSystem) = {
    try {
      fs.close()
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }

  }

  /**
   *
   * is faster 36 times than spark repartition(1)
   * but is Incompatible with hadoop 3.0.0
   * https://stackoverflow.com/questions/42035735/how-to-do-copymerge-in-hadoop-3-0
   *
   * @param srcFS
   * @param srcDir  多个文件的父目录
   * @param dstFS
   * @param dstFile 目标文件
   * @param deleteSource
   * @param conf
   * @param addString
   * @return
   */
  def merge(srcFS: FileSystem, srcDir: String,
            dstFS: FileSystem, dstFile: String,
            deleteSource: Boolean,
            conf: Configuration, addString: String) = {
    try {

      FileUtil.copyMerge(srcFS, new Path(srcDir),
        dstFS, new Path(dstFile),
        true, new Configuration(), null)
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }

  }

}
