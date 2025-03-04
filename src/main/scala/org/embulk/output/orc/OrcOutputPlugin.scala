package org.embulk.output.orc

import java.io.IOException
import java.util
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.apache.hadoop.util.VersionInfo
import org.apache.orc.{CompressionKind, MemoryManager, OrcFile, TypeDescription, Writer}
import org.embulk.config.{ConfigSource, TaskReport, TaskSource}
import org.embulk.spi.{Exec, OutputPlugin, PageReader, Schema}
import org.embulk.util.aws.credentials.AwsCredentials

object OrcOutputPlugin {
  private[orc] def getSchema(schema: Schema) = {
    val oschema = TypeDescription.createStruct
    for (i <- 0 until schema.size) {
      val column = schema.getColumn(i)
      val `type` = column.getType
      `type`.getName match {
        case "long" =>
          oschema.addField(column.getName, TypeDescription.createLong)
        case "double" =>
          oschema.addField(column.getName, TypeDescription.createDouble)
        case "boolean" =>
          oschema.addField(column.getName, TypeDescription.createBoolean)
        case "string" =>
          oschema.addField(column.getName, TypeDescription.createString)
        case "timestamp" =>
          oschema.addField(column.getName, TypeDescription.createTimestamp)
        case _ =>
          System.out.println("Unsupported type")
      }
    }
    oschema
  }

  // We avoid using orc.MemoryManagerImpl since it is not threadsafe, but embulk is multi-threaded.
  // Embulk creates and uses multiple instances of TransactionalPageOutput in worker threads.
  // As a workaround, WriterLocalMemoryManager is bound to a single orc.Writer instance, and
  // notifies checkMemory() only to that instance.
  private class WriterLocalMemoryManager extends MemoryManager {
    final private[orc] val rowsBetweenChecks = 10000
    private var rowsAddedSinceCheck = 0
    private[orc] var boundCallback: MemoryManager.Callback = _

    @throws[IOException]
    override def addWriter(path: Path, requestedAllocation: Long, callback: MemoryManager.Callback): Unit = {
      if (boundCallback != null) {
        throw new IllegalStateException("WriterLocalMemoryManager should be bound to a single orc.Writer instance.")
      } else {
        boundCallback = callback
      }
    }

    @throws[IOException]
    override def removeWriter(path: Path): Unit = boundCallback = null

    @throws[IOException]
    override def addedRow(rows: Int): Unit = {
      rowsAddedSinceCheck += rows
      if (rowsAddedSinceCheck > rowsBetweenChecks) {
        boundCallback.checkMemory(1)
        rowsAddedSinceCheck = 0
      }
    }
  }

}

class OrcOutputPlugin extends OutputPlugin {
  override def transaction(config: ConfigSource, schema: Schema, taskCount: Int, control: OutputPlugin.Control) = {
    val task = config.loadConfig(classOf[PluginTask])
    // retryable (idempotent) output:
    // return resume(task.dump(), schema, taskCount, control);
    // non-retryable (non-idempotent) output:
    control.run(task.dump)
    Exec.newConfigDiff
  }

  override def resume(taskSource: TaskSource, schema: Schema, taskCount: Int, control: OutputPlugin.Control) = throw new UnsupportedOperationException("orc output plugin does not support resuming")

  override def cleanup(taskSource: TaskSource, schema: Schema, taskCount: Int, successTaskReports: util.List[TaskReport]): Unit = {
  }

  override def open(taskSource: TaskSource, schema: Schema, taskIndex: Int) = {
    val task = taskSource.loadTask(classOf[PluginTask])
    if (task.getOverwrite) {
      val credentials = AwsCredentials.getAWSCredentialsProvider(task).getCredentials
      OrcOutputPluginHelper.removeOldFile(buildPath(task, taskIndex), task)
    }
    val reader = new PageReader(schema)
    val writer = createWriter(task, schema, taskIndex)
    new OrcTransactionalPageOutput(reader, writer, task)
  }

  private def buildPath(task: PluginTask, processorIndex: Int): String = {
    val pathPrefix = task.getPathPrefix
    val pathSuffix = task.getFileNameExtension
    val sequenceFormat = task.getSequenceFormat
    val fmt = java.lang.String.format(sequenceFormat, processorIndex.asInstanceOf[AnyRef])
    pathPrefix + fmt + pathSuffix
  }

  private def getHadoopConfiguration(task: PluginTask) = {
    val conf = new Configuration
    // see: https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    // see: https://stackoverflow.com/questions/20833444/how-to-set-objects-in-hadoop-configuration
    AwsCredentials.getAWSCredentialsProvider(task)
    if (task.getAccessKeyId.isPresent) {
      conf.set("fs.s3a.access.key", task.getAccessKeyId.get)
      conf.set("fs.s3n.awsAccessKeyId", task.getAccessKeyId.get)
    }
    if (task.getSecretAccessKey.isPresent) {
      conf.set("fs.s3a.secret.key", task.getSecretAccessKey.get)
      conf.set("fs.s3n.awsSecretAccessKey", task.getSecretAccessKey.get)
    }
    if (task.getEndpoint.isPresent) {
      conf.set("fs.s3a.endpoint", task.getEndpoint.get)
      conf.set("fs.s3n.endpoint", task.getEndpoint.get)
    }
    conf
  }

  private def createWriter(task: PluginTask, schema: Schema, processorIndex: Int): Writer = {
    // val timestampFormatters = Timestamps.newTimestampColumnFormatters(task, schema, task.getColumnOptions)
    val conf = getHadoopConfiguration(task)
    val oschema = OrcOutputPlugin.getSchema(schema)
    // see: https://groups.google.com/forum/#!topic/vertx/lLb-slzpWVg
    Thread.currentThread.setContextClassLoader(classOf[VersionInfo].getClassLoader)

    var writer: Writer = null
    try { // Make writerOptions
      val writerOptions = createWriterOptions(task, conf)
      // see: https://stackoverflow.com/questions/9256733/how-to-connect-hive-in-ireport
      // see: https://community.hortonworks.com/content/kbentry/73458/connecting-dbvisualizer-and-datagrip-to-hive-with.html
      writer = OrcFile.createWriter(new Path(buildPath(task, processorIndex)), writerOptions.setSchema(oschema).memory(new OrcOutputPlugin.WriterLocalMemoryManager).version(OrcFile.Version.V_0_12))
    } catch {
      case e: IOException => throw e
    }
    writer
  }

  private def createWriterOptions(task: PluginTask, conf: Configuration) = {
    val bufferSize = task.getBufferSize
    val stripSize = task.getStripSize
    val blockSize = task.getBlockSize
    val kindString = task.getCompressionKind
    val kind = CompressionKind.valueOf(kindString)
    OrcFile.writerOptions(conf).bufferSize(bufferSize).blockSize(blockSize.toLong).stripeSize(stripSize.toLong).compress(kind)
  }
}
