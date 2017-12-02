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

    // ORC File options
    @Config("strip_size")
    @ConfigDefault("100000")
    Integer getStripSize();

    @Config("buffer_size")
    @ConfigDefault("10000")
    Integer getBufferSize();

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
