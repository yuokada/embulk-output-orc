package org.embulk.output.orc

import java.nio.charset.StandardCharsets

import org.apache.hadoop.hive.ql.exec.vector._
import org.embulk.spi.{Column, ColumnVisitor, PageReader}

class OrcColumnVisitor(val reader: PageReader, val batch: VectorizedRowBatch, val i: Integer) extends ColumnVisitor {
  override def booleanColumn(column: Column): Unit = {
    column match {
      case _ if reader.isNull(column) => batch.cols(column.getIndex).asInstanceOf[LongColumnVector].vector(i) = 0
      case _ if reader.getBoolean(column) => batch.cols(column.getIndex).asInstanceOf[LongColumnVector].vector(i) = 1
      case _ => batch.cols(column.getIndex).asInstanceOf[LongColumnVector].vector(i) = 0
    }
  }

  override def longColumn(column: Column): Unit = batch.cols(column.getIndex).asInstanceOf[LongColumnVector].vector(i) = reader.getLong(column)

  override def doubleColumn(column: Column): Unit = batch.cols(column.getIndex).asInstanceOf[DoubleColumnVector].vector(i) = reader.getDouble(column)

  override def stringColumn(column: Column): Unit = batch.cols(column.getIndex).asInstanceOf[BytesColumnVector].setVal(i, reader.getString(column).getBytes(StandardCharsets.UTF_8))

  override def timestampColumn(column: Column): Unit = if (reader.isNull(column)) batch.cols(column.getIndex).asInstanceOf[TimestampColumnVector].setNullValue(i)
  else {
    val timestamp = reader.getTimestamp(column)
    val ts = new java.sql.Timestamp(timestamp.getEpochSecond * 1000)
    batch.cols(column.getIndex).asInstanceOf[TimestampColumnVector].set(i, ts)
  }

  override def jsonColumn(column: Column) = throw new UnsupportedOperationException("orc output plugin does not support json type")
}