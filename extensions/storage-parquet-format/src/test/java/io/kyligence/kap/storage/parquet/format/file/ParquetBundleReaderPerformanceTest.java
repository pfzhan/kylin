package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Test;

public class ParquetBundleReaderPerformanceTest extends AbstractParquetFormatTest {
    public ParquetBundleReaderPerformanceTest() throws IOException {
        super();
        type = new MessageType("test", new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 26, "key"), new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 16, "m1"), new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 16, "m2"), new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 16, "m3"));
    }

    protected void writeRows(int rowCnt) throws Exception {
        ParquetRawWriter writer = new ParquetRawWriterBuilder().setConf(new Configuration()).setPath(path).setType(type).build();
        byte[] key = "abcdefghijklmnopqrstuvwxyz".getBytes();
        byte[] m = "aaaabbbbccccddddaaaabbbbccccddddaaaabbbbccccdddd".getBytes();
        long t = System.currentTimeMillis();
        for (int i = 0; i < rowCnt; ++i) {
            writer.writeRow(key, 0, 26, m, new int[] { 16, 16, 16 });
        }
        t = System.currentTimeMillis() - t;
        System.out.println("Write file speed " + ((float) rowCnt / 1000 / t) + "M/s");
        writer.close();
    }

    @Test
    public void ReadInBundle() throws Exception {
        writeRows(groupSize);

        ParquetBundleReader bundleReader = new ParquetBundleReaderBuilder().setPath(path).setConf(new Configuration()).build();

        long t = System.currentTimeMillis();
        for (int i = 0; i < groupSize; ++i) {
            List<Object> data = bundleReader.read();
        }
        t = System.currentTimeMillis() - t;
        System.out.println("Read file speed " + ((float) 1000 / t) + "M/s");
        bundleReader.close();
    }
}
