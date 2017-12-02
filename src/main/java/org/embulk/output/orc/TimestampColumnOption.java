package org.embulk.output.orc;

import com.google.common.base.Optional;
import org.embulk.config.Config;
import org.embulk.config.ConfigDefault;
import org.embulk.config.Task;
import org.embulk.spi.time.TimestampFormatter;
import org.joda.time.DateTimeZone;

import java.util.List;

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
