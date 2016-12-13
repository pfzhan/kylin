package io.kyligence.kap.storage.parquet.format.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.junit.After;
import org.junit.Before;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public abstract class AbstractParquetFormatTest extends LocalFileMetadataTestCase {
    protected Path path, indexPath;
    protected static String tempFilePath;
    protected int groupSize = ParquetConfig.PagesPerGroup * ParquetConfig.RowsPerPage;
    protected MessageType type;

    public AbstractParquetFormatTest() throws IOException {
        path = new Path("./a.parquet");
        indexPath = new Path("./a.parquetindex");
        cleanTestFile(path);
        type = new MessageType("test", new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 2, "key1"), new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "m1"), new PrimitiveType(Type.Repetition.REQUIRED, PrimitiveType.PrimitiveTypeName.BINARY, 1, "m2"));
    }

    @After
    public void cleanup() throws IOException {
        cleanTestFile(path);
        cleanAfterClass();
    }

    @Before
    public void setup() {
        createTestMetadata();
    }

    protected void writeRows(int rowCnt) throws Exception {
        ParquetRawWriter writer = new ParquetRawWriter.Builder().setConf(new Configuration()).setPath(path).setType(type).build();
        for (int i = 0; i < rowCnt; ++i) {
            writer.writeRow(new byte[] { 1, 2, 3 }, 1, 2, new byte[] { 4, 5 }, new int[] { 1, 1 });
        }
        writer.close();
    }

    protected void cleanTestFile(Path path) throws IOException {
        FileSystem fs = FileSystem.get(new Configuration());
        if (fs.exists(path)) {
            fs.delete(path, true);
        }

        if (fs.exists(indexPath)) {
            fs.delete(indexPath, true);
        }
    }
}
