package io.kyligence.kap.storage.parquet.pageindex.column;

import com.google.common.collect.Maps;
import io.kyligence.kap.storage.parquet.pageIndex.column.ColumnIndexReader;
import io.kyligence.kap.storage.parquet.pageIndex.column.ColumnIndexWriter;
import io.kyligence.kap.storage.parquet.pageIndex.column.ColumnSpec;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.Log4jConfigurer;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Created by dong on 6/19/16.
 */
public class ColumnIndexTest {
    Path indexPath = new Path("/tmp/testkylin/a.inv");

    @Test
    public void testRoundTrip() throws IOException {
        Log4jConfigurer.initLogger();
        // prepare data
        Map<ByteArray, Integer> data = Maps.newLinkedHashMap();
        for (int i = 0; i < 100; i++) {
//            for (int j = 0; j < 10; j++) {
//                for (int k = 0; k < 1; k++) {
                    data.put(new ByteArray(new byte[]{(byte) i}), i );
//                }
//            }
        }

        // write
        FSDataOutputStream outputStream = FileSystem.getLocal(HadoopUtil.getCurrentConfiguration()).create(indexPath);
        ColumnIndexWriter indexWriter = new ColumnIndexWriter(new ColumnSpec("0", 1, 0, false, 0), outputStream);
        for (Map.Entry<ByteArray, Integer> dataEntry : data.entrySet()) {
            indexWriter.appendToRow(dataEntry.getKey(), dataEntry.getValue());
        }
        indexWriter.close();
        outputStream.close();

        // read
        FSDataInputStream inputStream = FileSystem.getLocal(HadoopUtil.getCurrentConfiguration()).open(indexPath);
        ColumnIndexReader indexReader = new ColumnIndexReader(inputStream);
        System.out.println(indexReader.getNumberOfRows());
//        for (Map.Entry<ByteArray, Integer> dataEntry : data.entrySet()) {
//            long t0 = System.currentTimeMillis();
//            int row = indexReader.getRows(dataEntry.getKey()).toArray()[0];
//
//            int row1 = indexReader.lookupGtIndex(dataEntry.getKey()).toArray()[0];
//            int row2 = indexReader.lookupLtIndex(dataEntry.getKey()).toArray()[0];
////            System.out.print(row);
////            System.out.print(" - ");
////            System.out.println((System.currentTimeMillis() - t0));
//
//            assertEquals(dataEntry.getValue().intValue(), row);
//        }

        System.out.println(indexReader.getRows(new ByteArray(new byte[]{(byte) 100})));
    }
}
