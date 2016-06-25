package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class ParquetTarballReaderTest extends AbstractParquetFormatTest {
    private Path tarballPath = null;

    public ParquetTarballReaderTest() throws IOException {
        super();
        tarballPath = new Path("./a.parquettar");
    }

    @After
    public void cleanup() throws IOException {
        super.cleanup();
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(tarballPath)) {
            fs.delete(tarballPath, true);
        }
    }

    @Test
    public void TestTarballReader() throws Exception {
        writeRows(groupSize);
        appendFile(100);

        ParquetTarballReader reader = new ParquetTarballReader(new Configuration(), tarballPath, indexPath);
        for (int j = 0; j < ParquetConfig.PagesPerGroup; ++j) {
            GeneralValuesReader valuesReader = reader.getValuesReader(j, 0);
            for (int i = 0; i < ParquetConfig.RowsPerPage; ++i) {
                Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new byte[] { 2, 3 });
            }
            Assert.assertNull(valuesReader.readBytes());
        }

        Assert.assertNull(reader.getValuesReader(ParquetConfig.PagesPerGroup, 0));
    }

    private void appendFile(long length) throws IOException {
        byte[] content = new byte[(int) length - 8];
        FileSystem fs = FileSystem.get(new Configuration());
        FSDataOutputStream outs = fs.create(tarballPath);
        outs.writeLong(length);
        outs.write(content);

        ContentSummary cSummary = fs.getContentSummary(path);
        byte[] parquetBytes = new byte[(int) cSummary.getLength()];
        FSDataInputStream ins = fs.open(path);
        ins.read(parquetBytes);
        ins.close();

        outs.write(parquetBytes);
        outs.close();
    }
}
