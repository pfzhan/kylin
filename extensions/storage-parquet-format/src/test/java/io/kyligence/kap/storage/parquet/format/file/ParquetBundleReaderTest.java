package io.kyligence.kap.storage.parquet.format.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.api.Binary;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

public class ParquetBundleReaderTest extends AbstractParquetFormatTest{
    public ParquetBundleReaderTest() throws IOException {
        super();
    }

    @Test
    public void ReadInBundle() throws Exception{
        writeRows(groupSize - 1);

        ParquetBundleReader bundleReader = new ParquetBundleReaderBuilder().setPath(path).setConf(new Configuration()).build();
        List<Object> data = bundleReader.read();
        Assert.assertNotNull(data);
        Assert.assertArrayEquals(((Binary)data.get(0)).getBytes(), new byte[] {2, 3});

        for (int i = 0; i < (groupSize - 2); ++i) {
            bundleReader.read();
        }

        Assert.assertNull(bundleReader.read());
        bundleReader.close();
    }
}
