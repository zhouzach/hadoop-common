import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

object HdfsHelper {

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
  def renameBulk(fs: FileSystem, srcFilePattern: String, dstFilePrefix: String) = {

    val srcPath = new Path(srcFilePattern)

    try {
      fs.globStatus(srcPath).foreach { file =>
        val dstPath = new Path(dstFilePrefix + System.currentTimeMillis())
        fs.rename(file.getPath, dstPath);
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
    } finally {
      fs.close()
    }
  }

}
