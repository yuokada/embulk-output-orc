package org.embulk.output.orc

import java.io.IOException
import java.nio.file.{Files, Paths}

import com.amazonaws.auth.profile.ProfileCredentialsProvider
import com.amazonaws.services.s3.AmazonS3Client
import com.amazonaws.services.s3.model.DeleteObjectRequest
import com.google.common.base.Throwables

import scala.beans.BeanProperty

object OrcOutputPluginHelper {
  def removeOldFile(fpath: String, task: PluginTask) = {
    // NOTE: Delete a file if local-filesystem, not HDFS or S3.
    val schema = getSchema(fpath)
    if (isDeleteTarget(schema)) schema match {
      case "file" =>
        try Files.deleteIfExists(Paths.get(fpath))
        catch {
          case e: IOException => Throwables.throwIfUnchecked(e)
        }
      case "s3" | "s3n" | "s3a" =>
        val s3Url = parseS3Url(fpath)
        val s3client = new AmazonS3Client(new ProfileCredentialsProvider)
        if (task.getEndpoint.isPresent) s3client.setEndpoint(task.getEndpoint.get)
        s3client.deleteObject(new DeleteObjectRequest(s3Url.bucket, s3Url.key))
      case _ =>
    }
  }

  def isDeleteTarget(schema: String): Boolean = schema match {
    case "file" => true
    case "s3" | "s3n" | "s3a" => true
    case _ => false
  }

  def getSchema(fpath: String) = {
    val schema = fpath.split("://").toList.apply(0)
    schema match {
      case "s3" | "s3a" | "s3n" => schema
      case _ => {
        val path = Paths.get(fpath)
        path.getFileSystem.provider.getScheme
      }
    }
  }

  def parseS3Url(s3url: String) = {
    val parts = s3url.split("(://|/)").toList
    val bucket = parts.apply(1)
    val key = parts.slice(2, parts.size).mkString("/")
    new OrcOutputPluginHelper.AmazonS3URILikeObject(bucket, key)
  }

  case class AmazonS3URILikeObject(@BeanProperty bucket: String, @BeanProperty key: String)
}
