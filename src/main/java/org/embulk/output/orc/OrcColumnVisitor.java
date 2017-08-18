package org.embulk.output.orc;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;

public class OrcColumnVisitor implements ColumnVisitor
{
    private PageReader reader;
    VectorizedRowBatch batch;
    Integer finalI;

    public OrcColumnVisitor(PageReader pageReader, VectorizedRowBatch rowBatch, Page page, Integer i)
    {
        int size = page.getStringReferences().size();

        this.reader = pageReader;
        this.batch = rowBatch;
        this.finalI = i;
    }

    @Override
    public void booleanColumn(Column column)
    {
        if (reader.isNull(column)) {
            ((LongColumnVector) batch.cols[column.getIndex()]).vector[finalI] = 0;
        }
        else {
            ((LongColumnVector) batch.cols[column.getIndex()]).vector[finalI] = reader.getLong(column);
        }
    }

    @Override
    public void longColumn(Column column)
    {
        ((LongColumnVector) batch.cols[column.getIndex()]).vector[finalI] = reader.getLong(column);
    }

    @Override
    public void doubleColumn(Column column)
    {
        ((DoubleColumnVector) batch.cols[column.getIndex()]).vector[finalI] = reader.getDouble(column);
    }

    @Override
    public void stringColumn(Column column)
    {
        ((BytesColumnVector) batch.cols[column.getIndex()]).setVal(finalI,
                reader.getString(column).getBytes());
    }

    @Override
    public void timestampColumn(Column column)
    {

    }

    @Override
    public void jsonColumn(Column column)
    {
        // throw unsupported
    }
}
