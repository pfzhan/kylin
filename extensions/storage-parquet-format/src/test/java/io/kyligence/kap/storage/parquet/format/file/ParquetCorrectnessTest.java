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

public class ParquetCorrectnessTest {
    private Path path, indexPath;
    private static String tempFilePath;
    private int groupSize = ParquetConfig.PagesPerGroup * ParquetConfig.RowsPerPage;
    private MessageType type;

    public ParquetCorrectnessTest() throws IOException {
        path = new Path("./a.parquet");
        indexPath = new Path("./a.parquetindex");
        cleanTestFile(path);
        type = new MessageType("test",
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "key1"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "m1"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "m2"));
    }

    @After
    public void cleanup() throws IOException {
        cleanTestFile(path);
    }

    @Test
    public void ReadPageByPageIndex() throws Exception{
        ParquetWriter writer = new ParquetWriterBuilder().setConf(new Configuration())
                .setPath(path)
                .setType(type)
                .build();
        for (int i = 0; i < (groupSize - 1); ++i) {
            writer.writeRow(new byte[]{1, 2, 3}, 1, 2, new byte[]{4, 5}, new int[]{1, 1});
        }
        writer.close();

        ParquetReader reader = new ParquetReaderBuilder().setPath(path)
                .setConf(new Configuration())
                .build();
        GeneralValuesReader valuesReader = reader.getValuesReader(ParquetConfig.PagesPerGroup - 1, 0);
        Assert.assertArrayEquals(valuesReader.readBytes().getBytes(), new byte[] {2, 3});
        for (int i = 0; i < (ParquetConfig.RowsPerPage - 2); ++i) {
            Assert.assertNotNull(valuesReader.readBytes());
        }
        Assert.assertNull(valuesReader.readBytes());
        reader.close();
    }

    @Test
    public void ReadNextPage() throws Exception{
        ParquetWriter writer = new ParquetWriterBuilder().setConf(new Configuration())
                .setPath(path)
                .setType(type)
                .build();
        for (int i = 0; i < (groupSize - 1); ++i) {
            writer.writeRow(new byte[]{1, 2, 3}, 1, 2, new byte[]{4, 5}, new int[]{1, 1});
        }
        writer.close();

        ParquetReader reader = new ParquetReaderBuilder().setPath(path)
                .setConf(new Configuration())
                .build();
        for (int i = 0; i < ParquetConfig.PagesPerGroup; ++i) {
            GeneralValuesReader valuesReader = reader.getNextValuesReader();
            Assert.assertNotNull(valuesReader);
        }
        GeneralValuesReader valuesReader = reader.getNextValuesReader();
        Assert.assertNull(valuesReader);
        reader.close();
    }

    private void cleanTestFile(Path path) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        if (fs.exists(indexPath)) {
            fs.delete(path, true);
        }
    }
}
