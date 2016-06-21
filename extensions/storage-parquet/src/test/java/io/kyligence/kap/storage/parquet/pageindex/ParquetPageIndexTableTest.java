package io.kyligence.kap.storage.parquet.pageindex;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Random;

import io.kyligence.kap.storage.parquet.pageIndex.ParquetPageIndexTable;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Log4jConfigurer;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.storage.parquet.format.pageIndex.ParquetPageIndexWriter;

/**
 * Created by dong on 6/21/16.
 */
public class ParquetPageIndexTableTest extends LocalFileMetadataTestCase {
    File indexFile;
    FSDataInputStream inputStream;

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }

    @Before
    public void setUp() throws Exception {
        Log4jConfigurer.initLogger();
        createTestMetadata();

        indexFile = File.createTempFile("local", "inv");
        writeIndexFile();
        inputStream = FileSystem.get(HadoopUtil.getCurrentConfiguration()).open(new Path(indexFile.getAbsolutePath()));
    }

    private void writeIndexFile() throws IOException {
        // arrange
        int dataSize = 100;
        int totalPageNum = 150;

        int maxVal1 = 50;
        int maxVal2 = 100;
        int cardinality1 = 50;
        int cardinality2 = 100;

        int[] columnLength = { Integer.SIZE - Integer.numberOfLeadingZeros(maxVal1), Integer.SIZE - Integer.numberOfLeadingZeros(maxVal2) };
        int[] cardinality = { cardinality1, cardinality2 };
        String[] columnName = { "1", "2" };
        int[] data1 = new int[dataSize];
        int[] data2 = new int[dataSize];
        Random random = new Random();
        for (int i = 0; i < dataSize; i++) {
            data1[i] = random.nextInt(maxVal1);
            data2[i] = random.nextInt(maxVal2);
        }

        // write
        boolean[] onlyEq = { false, false };
        ParquetPageIndexWriter writer = new ParquetPageIndexWriter(columnName, columnLength, cardinality, onlyEq, new FSDataOutputStream(new FileOutputStream(indexFile)));
        for (int i = 0; i < dataSize; i++) {
            byte[] buffer = new byte[columnLength[0] + columnLength[1]];
            int pageId = random.nextInt(totalPageNum);
            BytesUtil.writeUnsigned(data1[i], buffer, 0, columnLength[0]);
            BytesUtil.writeUnsigned(data2[i], buffer, columnLength[0], columnLength[1]);
            writer.write(buffer, 0, pageId);
        }
        writer.close();
    }

    @Test
    public void testLookup() throws IOException {
        ParquetPageIndexTable indexTable = new ParquetPageIndexTable(inputStream);
        TupleFilter filter = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.EQ);
        filter.addChild(new ColumnTupleFilter(ColumnDesc.mockup(null, 1, "1", "int").getRef()));
        filter.addChild(new ConstantTupleFilter(new ByteArray(new byte[] { 0, 0, 0, 0, 0, 0 })));
        Iterable<Integer> result = indexTable.lookup(filter);
        for (int r : result) {
            System.out.println(r);
        }
    }
}
