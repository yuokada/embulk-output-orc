package org.embulk.output.orc;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.spi.time.TimestampFormatter;
import org.embulk.util.aws.credentials.AwsCredentialsTask;
import org.joda.time.DateTimeZone;

import java.util.Map;

public interface PluginTask
        extends Task, TimestampFormatter.Task, AwsCredentialsTask
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

    // see: https://orc.apache.org/docs/hive-config.html
    // ORC File options
    @Config("strip_size")
    @ConfigDefault("67108864") // 64MB
    Integer getStripSize();

    @Config("buffer_size")
    @ConfigDefault("262144") // 256KB
    Integer getBufferSize();

    @Config("block_size")
    @ConfigDefault("268435456") // 256MB
    Integer getBlockSize();

    @Config("compression_kind")
    @ConfigDefault("ZLIB")
    public String getCompressionKind();

    @Config("overwrite")
    @ConfigDefault("false")
    boolean getOverwrite();

    @Config("default_from_timezone")
    @ConfigDefault("\"UTC\"")
    DateTimeZone getDefaultFromTimeZone();

    @Config("endpoint")
    @ConfigDefault("null")
    Optional<String> getEndpoint();
}
