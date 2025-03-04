package org.embulk.output.orc

import java.util

import com.google.common.base.Optional
import org.embulk.config.{Config, ConfigDefault, Task}
// import org.embulk.spi.time.TimestampFormatter
import org.joda.time.DateTimeZone

/*
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
 */

trait TimestampColumnOption extends Task {
  @Config("from_timezone")
  @ConfigDefault("null")
  def getFromTimeZone: Optional[DateTimeZone]

  @Config("from_format")
  @ConfigDefault("null")
  def getFromFormat: Optional[util.List[String]]
}
