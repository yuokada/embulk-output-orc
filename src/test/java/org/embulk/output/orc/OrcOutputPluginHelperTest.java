package org.embulk.output.orc;

import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;

public class OrcOutputPluginHelperTest
{
    @DataProvider(name = "url-provider")
    public Object[][] dataProvider()
    {
        return new Object[][] {
                {"file://tmp/output.orc", "file"},
                {"/tmp/output.000.orc", "file"},
                {"s3n://embulk-test/output.0001.orc", "s3n"},
                {"s3a://embulk-test/output.0001.orc", "s3a"},
                {"s3://embulk-test/output.0001.orc", "s3"},
                };
    }

    @Test(dataProvider = "url-provider")
    public void getFPathTest(String file, String expect)
    {
        String schema = OrcOutputPluginHelper.getSchema(file);
        assertThat(schema, is(expect));
    }

    @DataProvider(name = "schema-provider")
    public Object[][] schemaProvider()
    {
        return new Object[][] {
                {"file", true},
                {"s3", false},
                {"s3n", false},
                {"s3a", false},
                {"hdfs", false},
                };
    }

    @Test(dataProvider = "schema-provider")
    public void isDeleteTargetTest(String schema, boolean expect)
    {
        boolean result = OrcOutputPluginHelper.isDeleteTarget(schema);
        assertThat(result, is(expect));
    }

    @DataProvider(name = "parserTest-provider")
    public Object[][] parserTestProvider()
    {
        String baseurl = "demo-bucket/test/output.000.orc";
        String bucket = "demo-bucket";
        String keyname = "test/output.000.orc";

        return new Object[][] {
                {"s3://" + baseurl, bucket, keyname},
                {"s3a://" + baseurl, bucket, keyname},
                {"s3n://" + baseurl, bucket, keyname},
                };
    }

    @Test(dataProvider = "parserTest-provider")
    public void parseS3UrlTest(String url, String bucket, String key)
    {
        OrcOutputPluginHelper.AmazonS3URILikeObject parts =
                OrcOutputPluginHelper.parseS3Url(url);
        assertThat(parts.getBucket(), is(bucket));
        assertThat(parts.getKey(), is(key));
    }
}
