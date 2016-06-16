package io.kyligence.kap.storage.parquet.format.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;

public class ParquetColumnReaderTest extends AbstractParquetFormatTest{

    public ParquetColumnReaderTest() throws IOException {
        super();
    }

    @Test
    public void TestGetNextValuesReader() throws Exception{
        writeRows(ParquetConfig.RowsPerPage);

        ParquetColumnReader reader = new ParquetColumnReaderBuilder().setPath(path)
                .setConf(new Configuration())
                .setColumn(0)
                .build();
        GeneralValuesReader valuesReader = reader.getNextValuesReader();
        Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new byte[] {2, 3});
        Assert.assertNull(reader.getNextValuesReader());
        reader.close();
    }
}
