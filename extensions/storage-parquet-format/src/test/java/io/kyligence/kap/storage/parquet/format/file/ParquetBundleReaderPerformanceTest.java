package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.values.ValuesReader;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.Ignore;
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
    @Ignore
    public void ReadInBundleCache() throws Exception {
        writeRows(groupSize);

        long t = System.currentTimeMillis();
        ParquetBundleReader bundleReader = new ParquetBundleReaderBuilder().setPath(new Path("/Users/roger/0.parquet")).setConf(new Configuration()).build();

        long t2 = System.currentTimeMillis() - t;
        System.out.println("Create takes " + t2 + " ms");

        while(bundleReader.read() != null);

        t = System.currentTimeMillis() - t;
        System.out.println("Take " + t + " ms");

        bundleReader.close();
    }

    @Test
    @Ignore
    public void ReadInBundle() throws Exception {
        writeRows(groupSize);

        long t = System.currentTimeMillis();
        ParquetBundleReader bundleReader = new ParquetBundleReaderBuilder().setPath(new Path("/Users/roger/0.parquet")).setConf(new Configuration()).build();

        long t2 = System.currentTimeMillis() - t;
        System.out.println("Create takes " + t2 + " ms");

        while(bundleReader.read() != null);

        t = System.currentTimeMillis() - t;
        System.out.println("Take " + t + " ms");
        bundleReader.close();
    }

    @Test
    @Ignore
    public void ReadInBundleColumn() throws Exception {
        writeRows(groupSize);

        long t = System.currentTimeMillis();
        List<ParquetColumnReader> readerList = new ArrayList<>();
        Path p = new Path("/Users/roger/0.parquet");
        Configuration c = new Configuration();
        for (int i = 0; i < 300; i++) {
            ParquetColumnReader columnReader = new ParquetColumnReaderBuilder().setColumn(i).setConf(c).setPath(p).build();
            readerList.add(columnReader);
        }

        long t2 = System.currentTimeMillis() - t;
        System.out.println("Create takes " + t2 + " ms");

        for (int i = 0; i < 300; i++) {
            ParquetColumnReader reader = readerList.get(i);
            GeneralValuesReader gReader;
            while ((gReader = reader.getNextValuesReader()) != null) {
                while (gReader.readData() != null);
            }
        }
        t = System.currentTimeMillis() - t;
        System.out.println("Take " + t + " ms");
    }
}
