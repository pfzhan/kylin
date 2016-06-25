package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class ParquetRawReaderTest extends AbstractParquetFormatTest {

    public ParquetRawReaderTest() throws IOException {
        super();
    }

    @Test
    public void TestColumnCount() throws Exception {
        writeRows(ParquetConfig.RowsPerPage);

        ParquetRawReader reader = new ParquetRawReaderBuilder().setPath(path).setConf(new Configuration()).build();
        Assert.assertEquals(reader.getColumnCount(), 3);
        reader.close();
    }

    @Test
    public void TestReadPageByGlobalPageIndex() throws Exception {
        writeRows(groupSize);

        ParquetRawReader reader = new ParquetRawReaderBuilder().setPath(path).setConf(new Configuration()).build();
        GeneralValuesReader valuesReader = reader.getValuesReader(ParquetConfig.PagesPerGroup - 1, 0);
        Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new byte[] { 2, 3 });
        for (int i = 0; i < (ParquetConfig.RowsPerPage - 1); ++i) {
            Assert.assertNotNull(valuesReader.readBytes());
        }
        Assert.assertNull(valuesReader.readBytes());
        reader.close();
    }

    @Test
    public void TestReadPageByGroupAndPageIndex() throws Exception {
        writeRows(groupSize);
        ParquetRawReader reader = new ParquetRawReaderBuilder().setPath(path).setConf(new Configuration()).build();
        GeneralValuesReader valuesReader = reader.getValuesReader(0, 0, 1);
        Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new byte[] { 2, 3 });
        for (int i = 0; i < (ParquetConfig.RowsPerPage - 1); ++i) {
            Assert.assertNotNull(valuesReader.readBytes());
        }
        Assert.assertNull(valuesReader.readBytes());
        reader.close();
    }

}
