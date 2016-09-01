package io.kyligence.kap.storage.parquet.format.pageIndex;

import java.io.FileOutputStream;
import java.util.List;

import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReader;
import io.kyligence.kap.storage.parquet.format.file.ParquetBundleReaderBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.io.api.Binary;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;
import io.kyligence.kap.storage.parquet.format.pageIndex.column.ColumnSpec;
import io.kyligence.kap.storage.parquet.format.raw.RawTableUtils;

@Ignore("This is used for debugging index.")
public class ParquetPageIndexLocalTest extends LocalFileMetadataTestCase {

    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @Test
    public void BuildIndex() throws Exception {
        //        writeRows(ParquetConfig.RowsPerPage);
        ParquetBundleReader reader = new ParquetBundleReaderBuilder().setPath(new Path("/Users/dong/Documents/9.parquet")).setConf(new Configuration()).build();
        String[] name = { "puttime", "key", "uid", "tbl", "fsize", "hash", "md5", "ip", "mimetype" };
        int[] length = { 8, 8, 8, 8, 8, 8, 8, 8, 8 };
        int[] card = { 1, 1, 1, 1, 1, 1, 1, 1, 1 };
        boolean[] onlyEQ = { true, true, true, true, true, true, true, true, true };

        ColumnSpec[] specs = new ColumnSpec[9];
        for (int i = 0; i < 9; i++) {
            specs[i] = new ColumnSpec(name[i], length[i], card[i], onlyEQ[i], i);
            //            specs[i].setKeyEncodingIdentifier('a');
            //            specs[i].setValueEncodingIdentifier('s');
        }
        ColumnSpec[] specsSub = new ColumnSpec[1];
        specsSub[0] = new ColumnSpec("key", 6, 1, true, 1);
        ParquetPageIndexWriter indexWriter = new ParquetPageIndexWriter(specs, new FSDataOutputStream(new FileOutputStream("/tmp/new2.parquetindex")));
        ParquetPageIndexWriter indexWriterSub = new ParquetPageIndexWriter(specsSub, new FSDataOutputStream(new FileOutputStream("/tmp/new3.parquetindex")));

        int i = 0;
        while (true) {
            if (i++ % 100000 == 0) {
                System.out.println(i);
            }
            List<Object> readObjects = reader.read();
            if (readObjects == null) {
                break;
            }
            List<byte[]> byteList = Lists.newArrayListWithExpectedSize(readObjects.size());
            for (Object obj : readObjects) {
                byteList.add(((Binary) obj).getBytes());
            }
            byteList = RawTableUtils.hash(byteList);
            int pageIndex = reader.getPageIndex();
            indexWriter.write(byteList, pageIndex);

            writeSubstring(indexWriterSub, byteList.get(1), pageIndex, 6);
        }

        indexWriter.close();
        indexWriterSub.close();
        reader.close();
    }

    private void writeSubstring(ParquetPageIndexWriter writer, byte[] value, int pageId, int length) {
        // skip if value's length is less than required length
        if (value.length < length) {
            return;
        }

        for (int index = 0; index <= (value.length - length); index++) {
            writer.write(value, index, pageId);
        }
    }
}
