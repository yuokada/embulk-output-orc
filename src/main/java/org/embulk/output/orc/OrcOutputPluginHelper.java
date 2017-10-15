package org.embulk.output.orc;

import com.google.common.base.Throwables;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

class OrcOutputPluginHelper
{
    protected OrcOutputPluginHelper()
    {
        throw new UnsupportedOperationException();
    }

    static void removeOldFile(String fpath)
    {
        Path path = Paths.get(fpath);
        // TODO: Check local file. not HDFS or S3.
        try {
            Files.deleteIfExists(path);
        }
        catch (IOException e) {
            Throwables.propagate(e);
        }
    }
}
