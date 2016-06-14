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

public class ParquetBundleReaderTest {
    private Path path, indexPath;
    private static String tempFilePath;
    private int groupSize = ParquetConfig.PagesPerGroup * ParquetConfig.RowsPerPage;
    private MessageType type;

    public ParquetBundleReaderTest() throws IOException {
        path = new Path("./a.parquet");
        indexPath = new Path("./a.parquetindex");
        cleanTestFile(path);
        type = new MessageType("test",
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "key1"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "key2"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "m1"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "m2"));
    }

    @After
    public void cleanup() throws IOException {
        cleanTestFile(path);
    }

    @Test
    public void ReadInBundle() throws Exception{
        ParquetWriter writer = new ParquetWriterBuilder().setConf(new Configuration())
                .setPath(path)
                .setType(type)
                .build();
        for (int i = 0; i < (groupSize - 1); ++i) {
            writer.writeRow(new byte[]{1, 2, 3}, 1, 2, new byte[]{4, 5}, new int[]{1, 1});
        }
        writer.close();

        ParquetBundleReader bundleReader = new ParquetBundleReaderBuilder().setPath(path).setConf(new Configuration()).build();
        List<Object> data = bundleReader.read();
        Assert.assertNotNull(data);
        Assert.assertArrayEquals(((Binary)data.get(0)).getBytes(), new byte[] {2});

        for (int i = 0; i < (groupSize - 2); ++i) {
            bundleReader.read();
        }

        Assert.assertNull(bundleReader.read());
        bundleReader.close();
    }

    private void cleanTestFile(Path path) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(path)) {
            fs.deleteOnExit(path);
        }

        if (fs.exists(indexPath)) {
            fs.deleteOnExit(indexPath);
        }
    }
}
