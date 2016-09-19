package io.kyligence.kap.storage.parquet.format.pageIndex;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class ParquetPageIndexWriteReadTest extends LocalFileMetadataTestCase {
    @AfterClass
    public static void after() throws Exception {
        cleanAfterClass();
    }

    @BeforeClass
    public static void setUp() throws Exception {
        staticCreateTestMetadata();
    }

    @Test
    public void testWrite() throws IOException {
        // arrange
        int dataSize = 100;
        int totalPageNum = 150;

        int maxVal1 = 50;
        int maxVal2 = 100;
        int cardinality1 = 50;
        int cardinality2 = 100;

        int[] columnLength = { (Integer.SIZE - Integer.numberOfLeadingZeros(maxVal1) + 7) / 8, (Integer.SIZE - Integer.numberOfLeadingZeros(maxVal2) + 7) / 8 };
        int[] cardinality = { cardinality1, cardinality2 };
        String[] columnName = { "1", "2" };
        int[] data1 = new int[dataSize];
        int[] data2 = new int[dataSize];
        Map<Integer, Set<Integer>> dataMap1 = Maps.newLinkedHashMap();
        Map<Integer, Set<Integer>> dataMap2 = Maps.newLinkedHashMap();
        Random random = new Random();
        for (int i = 0; i < dataSize; i++) {
            data1[i] = random.nextInt(maxVal1);
            data2[i] = random.nextInt(maxVal2);
        }
        Set<Integer> s1 = Sets.newHashSet();
        for (int a : data1) {
            s1.add(a);
        }
        Set<Integer> s2 = Sets.newHashSet();
        for (int a : data2) {
            s2.add(a);
        }
        File indexFile = File.createTempFile("local", "inv");
        System.out.println("Temp index file: " + indexFile);

        // write
        boolean[] onlyEq = { false, false };
        ParquetPageIndexWriter writer = new ParquetPageIndexWriter(columnName, columnLength, cardinality, onlyEq, new FSDataOutputStream(new FileOutputStream(indexFile)));
        for (int i = 0; i < dataSize; i++) {
            byte[] buffer1 = new byte[columnLength[0]];
            byte[] buffer2 = new byte[columnLength[1]];
            int pageId = random.nextInt(totalPageNum);
            BytesUtil.writeUnsigned(data1[i], buffer1, 0, columnLength[0]);
            BytesUtil.writeUnsigned(data2[i], buffer2, 0, columnLength[1]);
            writer.write(Lists.newArrayList(buffer1, buffer2), pageId);

            if (!dataMap1.containsKey(data1[i])) {
                dataMap1.put(data1[i], Sets.<Integer> newLinkedHashSet());
            }
            dataMap1.get(data1[i]).add(pageId);
            if (!dataMap2.containsKey(data2[i])) {
                dataMap2.put(data2[i], Sets.<Integer> newLinkedHashSet());
            }
            dataMap2.get(data2[i]).add(pageId);
        }
        writer.close();

        // read
        FSDataInputStream inputStream = FileSystem.get(HadoopUtil.getCurrentConfiguration()).open(new Path(indexFile.getAbsolutePath()));
        ParquetPageIndexReader reader = new ParquetPageIndexReader(inputStream, 0);

        for (int i = 0; i < data1.length; i++) {
            byte[] buffer = new byte[columnLength[0]];
            Set<Integer> expected = dataMap1.get(data1[i]);

            BytesUtil.writeUnsigned(data1[i], buffer, 0, columnLength[0]);
            Set<Integer> actual = Sets.newHashSet(reader.readColumnIndex(0).getRows(new ByteArray(buffer)));
            assertEquals(expected, actual);
        }

        for (int i = 0; i < data1.length; i++) {
            byte[] buffer = new byte[columnLength[1]];
            Set<Integer> expected = dataMap2.get(data2[i]);

            BytesUtil.writeUnsigned(data2[i], buffer, 0, columnLength[1]);
            Set<Integer> actual = Sets.newHashSet(reader.readColumnIndex(1).getRows(new ByteArray(buffer)));
            assertEquals(expected, actual);
        }
        System.out.println(reader.getPageTotalNum(0));
        System.out.println(reader.getPageTotalNum(1));
        reader.close();
        inputStream.close();
    }
}
