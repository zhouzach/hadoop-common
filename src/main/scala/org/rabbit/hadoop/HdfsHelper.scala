package org.rabbit.hadoop

import java.io.IOException
import java.net.URI

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.permission.AclEntry
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
   * @param srcPath
   * @param dstPathPrefix
   */
  def renameBulk(fs: FileSystem, srcPath: String, dstPathPrefix: String, dstFileSuffix: String) = {

    val srcFilePattern = new Path(s"$srcPath/part*")

    try {
      fs.globStatus(srcFilePattern).foreach { file =>
        val dstPath = new Path(dstPathPrefix + System.currentTimeMillis() + dstFileSuffix)
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
   * is faster 36 times than org.rabbit.spark repartition(1)
   * but is Incompatible with org.rabbit.hadoop 3.0.0
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

  /**
   * Parses a string representation of an ACL spec into a list of AclEntry
   * objects. Example: "user::rwx,user:foo:rw-,group::r--,other::---"
   *
   * @param aclSpec
   * String representation of an ACL spec.
   * @param includePermission
   * for setAcl operations this will be true. i.e. AclSpec should
   * include permissions.<br>
   * But for removeAcl operation it will be false. i.e. AclSpec should
   * not contain permissions.<br>
   * Example: "user:foo,group:bar"
   */
  def modifyAcl(fs: FileSystem, pathStr: String, aclSpec: String, includePermission: Boolean) = {
    try {
      val path = new Path(pathStr)
      println(s"acl print: ${fs.getAclStatus(path)}")
      fs.modifyAclEntries(path, AclEntry.parseAclSpec(aclSpec, includePermission))
      //      fs.setAcl(path, AclEntry.parseAclSpec(aclSpec, includePermission))
      println(s"new acl print: ${fs.getAclStatus(path)}")
    } catch {
      case ex: UnsupportedOperationException =>
        ex.printStackTrace()
      case e: IOException =>
        e.printStackTrace()
    } finally {
      fs.close()
    }
  }

}
