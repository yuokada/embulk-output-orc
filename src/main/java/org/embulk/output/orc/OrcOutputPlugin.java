package org.embulk.output.orc;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DoubleColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.util.VersionInfo;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.ConfigDiff;
import org.embulk.config.ConfigSource;
import org.embulk.config.Task;
import org.embulk.config.TaskReport;
import org.embulk.config.TaskSource;
import org.embulk.spi.Column;
import org.embulk.spi.ColumnVisitor;
import org.embulk.spi.Exec;
import org.embulk.spi.OutputPlugin;
import org.embulk.spi.Page;
import org.embulk.spi.PageReader;
import org.embulk.spi.Schema;
import org.embulk.spi.TransactionalPageOutput;
import org.embulk.spi.time.Timestamp;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.spi.type.Type;
import org.embulk.spi.util.Timestamps;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class OrcOutputPlugin
        implements OutputPlugin
{
    public interface PluginTask
            extends Task, TimestampFormatter.Task
    {
        @Config("path_prefix")
        String getPathPrefix();

        @Config("file_ext")
        @ConfigDefault("\".orc\"")
        String getFileNameExtension();

        @Config("column_options")
        @ConfigDefault("{}")
        Map<String, TimestampColumnOption> getColumnOptions();

        @Config("sequence_format")
        @ConfigDefault("\".%03d\"")
        String getSequenceFormat();

        @Config("overwrite")
        @ConfigDefault("false")
        boolean getOverwrite();

        @Config("default_from_timezone")
        @ConfigDefault("\"UTC\"")
        DateTimeZone getDefaultFromTimeZone();
    }

    public interface TimestampColumnOption
            extends Task, TimestampFormatter.TimestampColumnOption
    {
        @Config("from_timezone")
        @ConfigDefault("null")
        Optional<DateTimeZone> getFromTimeZone();

        @Config("from_format")
        @ConfigDefault("null")
        Optional<List<String>> getFromFormat();
    }

    @Override
    public ConfigDiff transaction(ConfigSource config,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        PluginTask task = config.loadConfig(PluginTask.class);

        // retryable (idempotent) output:
        // return resume(task.dump(), schema, taskCount, control);

        // non-retryable (non-idempotent) output:
        control.run(task.dump());
        return Exec.newConfigDiff();
    }

    @Override
    public ConfigDiff resume(TaskSource taskSource,
            Schema schema, int taskCount,
            OutputPlugin.Control control)
    {
        throw new UnsupportedOperationException("orc output plugin does not support resuming");
    }

    @Override
    public void cleanup(TaskSource taskSource,
            Schema schema, int taskCount,
            List<TaskReport> successTaskReports)

    {
    }

    @Override
    public TransactionalPageOutput open(TaskSource taskSource, Schema schema, int taskIndex)
    {
        PluginTask task = taskSource.loadTask(PluginTask.class);

        final PageReader reader = new PageReader(schema);
        Writer writer = createWriter(task, schema, taskIndex);

        return new OrcTransactionalPageOutput(reader, writer, task);
        // Write your code here :)
//        throw new UnsupportedOperationException("OrcOutputPlugin.run method is not implemented yet");
    }

    private String buildPath(PluginTask task, int processorIndex)
    {
        final String pathPrefix = task.getPathPrefix();
        final String pathSuffix = task.getFileNameExtension();
        final String sequenceFormat = task.getSequenceFormat();
        return pathPrefix + String.format(sequenceFormat, processorIndex) + pathSuffix;
    }

    private TypeDescription getSchema(Schema schema)
    {
        TypeDescription oschema = TypeDescription.createStruct();
        for (int i = 0; i < schema.size(); i++) {
            Column column = schema.getColumn(i);
            Type type = column.getType();
            switch (type.getName()) {
                case "long":
                    oschema.addField(column.getName(), TypeDescription.createLong());
                    break;
                case "double":
                    oschema.addField(column.getName(), TypeDescription.createDouble());
                    break;
                case "boolean":
                    oschema.addField(column.getName(), TypeDescription.createBoolean());
                    break;
                case "string":
                    oschema.addField(column.getName(), TypeDescription.createString());
                    break;
                case "timestamp":
                    oschema.addField(column.getName(), TypeDescription.createTimestamp());
                    break;
                default:
                    System.out.println("Unsupported type");
                    break;
            }
        }
        return oschema;
    }

    private Configuration getHadoopConfiguration()
    {
        Configuration conf = new Configuration();

        // see: https://stackoverflow.com/questions/17265002/hadoop-no-filesystem-for-scheme-file
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("fs.file.impl", LocalFileSystem.class.getName());
        // see: https://stackoverflow.com/questions/20833444/how-to-set-objects-in-hadoop-configuration

        return conf;
    }

    private Writer createWriter(PluginTask task, Schema schema, int processorIndex)
    {
        final TimestampFormatter[] timestampFormatters = Timestamps.newTimestampColumnFormatters(task, schema, task.getColumnOptions());

        Configuration conf = getHadoopConfiguration();
        TypeDescription oschema = getSchema(schema);

        // see: https://groups.google.com/forum/#!topic/vertx/lLb-slzpWVg
        Thread.currentThread().setContextClassLoader(VersionInfo.class.getClassLoader());

        Writer writer = null;
        try {
            // see: https://stackoverflow.com/questions/9256733/how-to-connect-hive-in-ireport
            // see: https://community.hortonworks.com/content/kbentry/73458/connecting-dbvisualizer-and-datagrip-to-hive-with.html
            writer = OrcFile.createWriter(new Path(buildPath(task, processorIndex)),
                    OrcFile.writerOptions(conf)
                            .setSchema(oschema)
                            .compress(CompressionKind.ZLIB)
                            .version(OrcFile.Version.V_0_12));
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return writer;
    }

    class OrcTransactionalPageOutput
            implements TransactionalPageOutput
    {
        private PageReader reader;
        private Writer writer;
        private DateTimeFormatter formatter;

        public OrcTransactionalPageOutput(PageReader reader, Writer writer, PluginTask task)
        {
            this.reader = reader;
            this.writer = writer;

            // formatter
            DateTimeZone defaultTimeZone = DateTimeZone.forTimeZone(task.getDefaultFromTimeZone().toTimeZone());
            formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss").withZone(defaultTimeZone);
        }

        @Override
        public void add(Page page)
        {
            List<String> strings = page.getStringReferences();
            TypeDescription schema = getSchema(reader.getSchema());
            VectorizedRowBatch batch = schema.createRowBatch();
            batch.size = strings.size();

            reader.setPage(page);
            int i = 0;
            while (reader.nextRecord()) {
                // batch.size = page.getStringReferences().size();
                final int finalI = i;

                reader.getSchema().visitColumns(new ColumnVisitor()
                {

                    @Override
                    public void booleanColumn(Column column)
                    {
                        if (reader.isNull(column)) {
                            ((LongColumnVector) batch.cols[column.getIndex()]).vector[finalI] = 0;
                        }
                        else {
                            // TODO; Fix all true bug
                            if (reader.getBoolean(column)) {
                                ((LongColumnVector) batch.cols[column.getIndex()]).vector[finalI] = 1;
                            }
                            else {
                                ((LongColumnVector) batch.cols[column.getIndex()]).vector[finalI] = 0;
                            }
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
                        if (reader.isNull(column)) {
                            ((TimestampColumnVector) batch.cols[column.getIndex()]).setNullValue(finalI);
                        }
                        else {
                            Timestamp timestamp = reader.getTimestamp(column);
                            if (!timestamp.equals("")) {
                                java.sql.Timestamp ts = new java.sql.Timestamp(timestamp.getEpochSecond() * 1000);
                                ((TimestampColumnVector) batch.cols[column.getIndex()]).set(finalI, ts);
                            }
                            // throw new UnsupportedOperationException("orc output plugin does not support timestamp yet");
                        }
                    }

                    @Override
                    public void jsonColumn(Column column)
                    {
                        throw new UnsupportedOperationException("orc output plugin does not support json type");
                    }
                });
                i++;
            }
            try {
                writer.addRowBatch(batch);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void finish()
        {
            try {
                writer.close();
                writer = null;
            }
            catch (IOException e) {
                Throwables.propagate(e);
            }
        }

        @Override
        public void close()
        {
            // TODO: something
        }

        @Override
        public void abort()
        {
            // TODO: something
        }

        @Override
        public TaskReport commit()
        {
            // TODO: something
            return Exec.newTaskReport();
        }
    }
}
