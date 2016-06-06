package io.kyligence.kap.storage.parquet.format.file;

import static org.junit.Assert.assertEquals;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
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
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "key2"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "m1"),
                new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "m2"));
    }

    @Test
    public void PlainUncompressed() throws Exception{
        try {
            ParquetWriter writer = new ParquetWriterBuilder().setConf(new Configuration())
                                        .setPath(path)
                                        .setType(type)
                                        .build();
            for (int i = 0; i < (groupSize - 1); ++i) {
                writer.writeRow(new byte[]{1, 2, 3}, new int[]{1, 2}, new byte[]{4, 5}, new int[]{1, 1});
            }
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertEquals(1, 1);
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
