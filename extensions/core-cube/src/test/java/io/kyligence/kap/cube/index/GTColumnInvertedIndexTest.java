package io.kyligence.kap.cube.index;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Ints;

public class GTColumnInvertedIndexTest extends LocalFileMetadataTestCase {
    private static final Logger logger = LoggerFactory.getLogger(GTColumnInvertedIndexTest.class);

    CubeInstance cubeInstance;

    @Before
    public void setup() throws Exception {
        staticCreateTestMetadata("../../kylin/examples/test_case_data/localmeta");
        cubeInstance = mgr().getCube("test_kylin_cube_with_slr_ready");
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testBuildAndRead() throws IOException {
        CubeSegment cubeSegment = cubeInstance.getSegments().get(0);
        TblColRef tblColRef = cubeInstance.getAllDimensions().get(0);
        Dictionary<String> dict = cubeSegment.getDictionary(tblColRef);
        Map<Integer, Set<Integer>> dataMap = Maps.newHashMap();

        logger.info("build test data");
        Random random = new Random();
        int rowCounter = 0;
        for (int i = 0; i < dict.getSize(); i++) {
            Set s = Sets.newLinkedHashSet();
            int rowStep = random.nextInt(2);
            for (int r = 0; r < rowStep; r++) {
                s.add(r + rowCounter);
            }
            rowCounter += rowStep;
            dataMap.put(i, s);
        }

        logger.info("start to build index");
        File idxFile = File.createTempFile("tmp", ".idx");
        long initIdxFileLength = idxFile.length();

        GTColumnInvertedIndex index = new GTColumnInvertedIndex(idxFile.getAbsolutePath(), tblColRef.getName(), dict.getSize());
        GTColumnInvertedIndex.Builder builder = index.rebuild();
        for (int v = 0; v < dict.getSize(); v++) {
            for (int r : dataMap.get(v)) {
                builder.putNextRow(v);
            }
        }
        builder.close();

        assertTrue(initIdxFileLength < idxFile.length());

        logger.info("start to read index");
        GTColumnInvertedIndex.Reader reader = index.getReader();
        for (int v = dict.getMinId(); v < dict.getMaxId(); v++) {
            assertTrue(dataMap.get(v).containsAll(Ints.asList(reader.getRows(v).toArray())));
        }

        idxFile.delete();
    }

    @Test
    public void testBuildAndReadMultiValuePerRow() throws IOException {
        int indexTotalNum = 10000;

        CubeSegment cubeSegment = cubeInstance.getSegments().get(0);
        TblColRef tblColRef = cubeInstance.getAllDimensions().get(0);
        Dictionary<String> dict = cubeSegment.getDictionary(tblColRef);
        Map<Integer, Set<Integer>> resultMap = Maps.newHashMap();

        logger.info("start to build index");

        File idxFile = File.createTempFile("tmp", ".idx");
        long initIdxFileLength = idxFile.length();

        GTColumnInvertedIndex index = new GTColumnInvertedIndex(idxFile.getAbsolutePath(), tblColRef.getName(), dict.getSize());
        GTColumnInvertedIndex.Builder builder = index.rebuild();

        Random random = new Random();
        for (int i = 0; i < indexTotalNum; i++) {
            int arrNum = random.nextInt(3);
            Integer[] randArr = new Integer[arrNum];
            for (int j = 0; j < arrNum; j++) {
                randArr[j] = random.nextInt(dict.getSize());
                if (!resultMap.containsKey(j)) {
                    resultMap.put(j, Sets.<Integer> newLinkedHashSet());
                }
                resultMap.get(j).add(i);
            }
            builder.putNextRow(randArr);
        }

        builder.close();

        assertTrue(initIdxFileLength < idxFile.length());

        logger.info("start to read index");
        GTColumnInvertedIndex.Reader reader = index.getReader();
        for (int v : resultMap.keySet()) {
            assertTrue(resultMap.get(v).containsAll(Ints.asList(reader.getRows(v).toArray())));
        }

        idxFile.delete();
    }

    private CubeManager mgr() {
        return CubeManager.getInstance(getTestConfig());
    }
}
