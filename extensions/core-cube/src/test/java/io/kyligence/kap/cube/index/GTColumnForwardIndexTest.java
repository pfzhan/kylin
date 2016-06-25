package io.kyligence.kap.cube.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.apache.kylin.common.util.Dictionary;
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

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class GTColumnForwardIndexTest extends LocalFileMetadataTestCase {
    private static final Logger logger = LoggerFactory.getLogger(GTColumnForwardIndexTest.class);
    private static final int ROW_NUM = 10000000;

    CubeInstance cubeInstance;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        cubeInstance = mgr().getCube("test_kylin_cube_with_slr_ready");
    }

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }

    private CubeManager mgr() {
        return CubeManager.getInstance(getTestConfig());
    }

    @Test
    public void testBuildAndRead() throws IOException {
        CubeSegment cubeSegment = cubeInstance.getSegments().get(0);
        TblColRef tblColRef = cubeInstance.getAllDimensions().get(0);
        Dictionary<String> dict = cubeSegment.getDictionary(tblColRef);
        Map<Integer, Integer> dataMap = Maps.newHashMap();

        logger.info("build test data");
        Random random = new Random();
        for (int r = 0; r < ROW_NUM; r++) {
            dataMap.put(r, random.nextInt(dict.getSize() + dict.getMinId()));
        }

        logger.info("start to build index");
        File idxFile = File.createTempFile("tmp", ".idx");
        long initIdxFileLength = idxFile.length();

        GTColumnForwardIndex index = new GTColumnForwardIndex(tblColRef.getName(), dict.getMaxId(), idxFile.getAbsolutePath());
        GTColumnForwardIndex.Builder builder = index.rebuild();
        for (int r = 0; r < ROW_NUM; r++) {
            builder.putNextRow(dataMap.get(r));
        }
        logger.info("written {} records", ROW_NUM);
        builder.close();

        assertTrue(initIdxFileLength < idxFile.length());

        logger.info("start to read index");
        GTColumnForwardIndex.Reader reader = index.getReader();
        for (int r = 0; r < ROW_NUM; r++) {
            assertEquals((int) dataMap.get(r), reader.get(r));
        }

        idxFile.delete();
    }
}