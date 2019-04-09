package org.embulk.output.orc

import java.io.IOException

import com.google.common.base.Throwables
import org.apache.orc.Writer
import org.embulk.config.TaskReport
import org.embulk.spi.{Exec, Page, PageReader, TransactionalPageOutput}

class OrcTransactionalPageOutput(val reader: PageReader, val writer: Writer, val task: PluginTask) extends TransactionalPageOutput {
  override def add(page: Page): Unit = synchronized {
    try {
      // int size = page.getStringReferences().size();
      val schema = OrcOutputPlugin.getSchema(reader.getSchema)
      val batch = schema.createRowBatch
      // batch.size = size;
      reader.setPage(page)
      while ( {
        reader.nextRecord
      }) {
        val row = {
          batch.size += 1;
          batch.size - 1
        }
        reader.getSchema.visitColumns(new OrcColumnVisitor(reader, batch, row))
        if (batch.size >= batch.getMaxSize) {
          writer.addRowBatch(batch)
          batch.reset()
        }
      }
      if (batch.size != 0) {
        writer.addRowBatch(batch)
        batch.reset()
      }
    } catch {
      case e: IOException =>
        e.printStackTrace()
    }
  }

  override def finish(): Unit = {
    try writer.close()
    catch {
      case e: IOException => Throwables.throwIfUnchecked(e)
    }
  }

  override def close(): Unit = {}

  override def abort(): Unit = {}

  override def commit: TaskReport = Exec.newTaskReport
}