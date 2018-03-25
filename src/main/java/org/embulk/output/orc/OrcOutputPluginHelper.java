package org.embulk.output.orc;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.DeleteObjectRequest;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

class OrcOutputPluginHelper
{
    protected OrcOutputPluginHelper()
    {
        throw new UnsupportedOperationException();
    }

    static void removeOldFile(String fpath, PluginTask task)
    {
        // NOTE: Delete a file if local-filesystem, not HDFS or S3.
        String schema = getSchema(fpath);
        if (isDeleteTarget(schema)) {
            switch (schema) {
                case "file":
                    try {
                        Files.deleteIfExists(Paths.get(fpath));
                    }
                    catch (IOException e) {
                        Throwables.propagate(e);
                    }
                    break;
                case "s3":
                case "s3n":
                case "s3a":
                    AmazonS3URILikeObject s3Url = parseS3Url(fpath);
                    AmazonS3 s3client = new AmazonS3Client(new ProfileCredentialsProvider());
                    if (task.getEndpoint().isPresent()) {
                        s3client.setEndpoint(task.getEndpoint().get());
                    }
                    s3client.deleteObject(new DeleteObjectRequest(s3Url.getBucket(), s3Url.getKey()));
                default:
                    // TODO: Unsupported
            }
        }
    }

    public static boolean isDeleteTarget(String schema)
    {
        switch (schema) {
            case "file":
                return true;
            case "s3":
            case "s3a":
            case "s3n":
                return true;
            default:
                return false;
        }
    }

    static String getSchema(String fpath)
    {
        String schema = Splitter.on("://")
                .splitToList(fpath).get(0);
        if (schema.equals("s3a") || schema.equals("s3n") || schema.equals("s3")) {
            return schema;
        }
        else {
            Path path = Paths.get(fpath);
            return path.getFileSystem().provider().getScheme();
        }
    }

    static AmazonS3URILikeObject parseS3Url(String s3url)
    {
        List<String> parts = Arrays.asList(
                s3url.split("(://|/)"));
        String bucket = parts.get(1);
        String key = Joiner.on("/").join(parts.subList(2, parts.size()));
        return new AmazonS3URILikeObject(bucket, key);
    }

    static class AmazonS3URILikeObject
    {
        String bucket;
        String key;

        public AmazonS3URILikeObject(String bucket, String key)
        {
            this.bucket = bucket;
            this.key = key;
        }

        public String getBucket()
        {
            return bucket;
        }

        public String getKey()
        {
            return key;
        }
    }
}
