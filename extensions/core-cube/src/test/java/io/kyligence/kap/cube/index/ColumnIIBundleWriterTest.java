package io.kyligence.kap.cube.index;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Log4jConfigurer;
import org.junit.BeforeClass;
import org.junit.Test;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.io.Files;

import io.kyligence.kap.cube.index.ColumnIndexFactory;
import io.kyligence.kap.cube.index.IColumnInvertedIndex;
import io.kyligence.kap.cube.index.ColumnIIBundleWriter;

public class ColumnIIBundleWriterTest {

    @BeforeClass
    public static void setupClass() {
        Log4jConfigurer.initLogger();
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

        int[] columnLength = { Integer.SIZE - Integer.numberOfLeadingZeros(maxVal1), Integer.SIZE - Integer.numberOfLeadingZeros(maxVal2) };
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

        File localIdxDir = Files.createTempDir();
        System.out.println("Temp index dir: " + localIdxDir.getAbsolutePath());

        // act
        ColumnIIBundleWriter writer = new ColumnIIBundleWriter(columnName, columnLength, cardinality, localIdxDir);
        for (int i = 0; i < dataSize; i++) {
            byte[] buffer = new byte[columnLength[0] + columnLength[1]];
            int pageId = random.nextInt(totalPageNum);
            BytesUtil.writeUnsigned(data1[i], buffer, 0, columnLength[0]);
            BytesUtil.writeUnsigned(data2[i], buffer, columnLength[0], columnLength[1]);
            writer.write(buffer, pageId);

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

        // assert
        IColumnInvertedIndex.Reader reader1 = ColumnIndexFactory.createLocalInvertedIndex(columnName[0], cardinality1, localIdxDir + "/" + columnName[0] + ".inv").getReader();
        IColumnInvertedIndex.Reader reader2 = ColumnIndexFactory.createLocalInvertedIndex(columnName[1], cardinality2, localIdxDir + "/" + columnName[1] + ".inv").getReader();
        for (int r = 0; r < dataSize; r++) {
            assertEquals(dataMap1.get(data1[r]), Sets.newHashSet(reader1.getRows(data1[r])));
            assertEquals(dataMap2.get(data2[r]), Sets.newHashSet(reader2.getRows(data2[r])));
        }
    }
}
