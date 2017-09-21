package org.embulk.output.orc;

import java.nio.charset.StandardCharsets;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.PageReader;
import org.embulk.spi.time.Timestamp;

public class OrcColumnVisitor
        implements ColumnVisitor
{
    private PageReader reader;
    private VectorizedRowBatch batch;
    private Integer i;

    public OrcColumnVisitor(PageReader pageReader, VectorizedRowBatch rowBatch, Integer i)
    {
        this.reader = pageReader;
        this.batch = rowBatch;
        this.i = i;
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (reader.isNull(column)) {
            ((LongColumnVector) batch.cols[column.getIndex()]).vector[i] = 0;
        }
        else {
            // TODO; Fix all true bug
            if (reader.getBoolean(column)) {
                ((LongColumnVector) batch.cols[column.getIndex()]).vector[i] = 1;
            }
            else {
                ((LongColumnVector) batch.cols[column.getIndex()]).vector[i] = 0;
            }
        }
    }

    @Override
    public void longColumn(Column column)
    {
        ((LongColumnVector) batch.cols[column.getIndex()]).vector[i] = reader.getLong(column);
    }

    @Override
    public void doubleColumn(Column column)
    {
        ((DoubleColumnVector) batch.cols[column.getIndex()]).vector[i] = reader.getDouble(column);
    }

    @Override
    public void stringColumn(Column column)
    {
        ((BytesColumnVector) batch.cols[column.getIndex()]).setVal(i,
                reader.getString(column).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void timestampColumn(Column column)
    {
        if (reader.isNull(column)) {
            ((TimestampColumnVector) batch.cols[column.getIndex()]).setNullValue(i);
        }
        else {
            Timestamp timestamp = reader.getTimestamp(column);
            if (!timestamp.equals("")) {
                java.sql.Timestamp ts = new java.sql.Timestamp(timestamp.getEpochSecond() * 1000);
                ((TimestampColumnVector) batch.cols[column.getIndex()]).set(i, ts);
            }
            // throw new UnsupportedOperationException("orc output plugin does not support timestamp yet");
        }
    }

    @Override
    public void jsonColumn(Column column)
    {
        throw new UnsupportedOperationException("orc output plugin does not support json type");
    }
}
