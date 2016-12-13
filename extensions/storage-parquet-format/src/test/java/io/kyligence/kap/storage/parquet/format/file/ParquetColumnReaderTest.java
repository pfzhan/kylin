package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Test;

public class ParquetColumnReaderTest extends AbstractParquetFormatTest {

    public ParquetColumnReaderTest() throws IOException {
        super();
    }

    @Test
    public void TestGetNextValuesReader() throws Exception {
        writeRows(ParquetConfig.RowsPerPage);

        ParquetColumnReader reader = new ParquetColumnReader.Builder().setPath(path).setConf(new Configuration()).setColumn(0).build();
        GeneralValuesReader valuesReader = reader.getNextValuesReader();
        Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new byte[] { 2, 3 });
        Assert.assertNull(reader.getNextValuesReader());
        reader.close();
    }
}
