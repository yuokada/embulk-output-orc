package org.embulk.output.orc

import java.util

import com.google.common.base.Optional
import org.embulk.config.{Config, ConfigDefault, Task}
import org.embulk.spi.time.TimestampFormatter
import org.embulk.util.aws.credentials.AwsCredentialsTask
import org.joda.time.DateTimeZone

trait PluginTask extends Task with TimestampFormatter.Task with AwsCredentialsTask {
  @Config("path_prefix")
  def getPathPrefix: String

  @Config("file_ext")
  @ConfigDefault("\".orc\"")
  def getFileNameExtension: String

  @Config("column_options")
  @ConfigDefault("{}")
  def getColumnOptions: util.Map[String, TimestampColumnOption]

  @Config("sequence_format")
  @ConfigDefault("\".%03d\"")
  def getSequenceFormat: String

  // see: https://orc.apache.org/docs/hive-config.html
  // ORC File options
  @Config("strip_size")
  @ConfigDefault("67108864") // 64MB
  def getStripSize: Integer

  @Config("buffer_size")
  @ConfigDefault("262144") // 256KB
  def getBufferSize: Integer

  @Config("block_size")
  @ConfigDefault("268435456") // 256MB
  def getBlockSize: Integer

  @Config("compression_kind")
  @ConfigDefault("ZLIB")
  def getCompressionKind: java.lang.String

  @Config("overwrite")
  @ConfigDefault("false")
  def getOverwrite: java.lang.Boolean

  @Config("default_from_timezone")
  @ConfigDefault("\"UTC\"")
  def getDefaultFromTimeZone: DateTimeZone

  @Config("endpoint")
  @ConfigDefault("null")
  def getEndpoint: Optional[String]
}
