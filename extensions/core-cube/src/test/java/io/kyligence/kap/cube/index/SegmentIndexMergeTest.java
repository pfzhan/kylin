package io.kyligence.kap.cube.index;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

/**
 */
public class SegmentIndexMergeTest extends LocalFileMetadataTestCase {
    private static final Logger logger = LoggerFactory.getLogger(SegmentIndexMergeTest.class);

    CubeInstance cubeInstance;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        cubeInstance = mgr().getCube("test_kylin_cube_without_slr_secondary_index");
    }

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }

    private CubeManager mgr() {
        return CubeManager.getInstance(getTestConfig());
    }

    @Test
    public void testMerge() throws IOException {

        CubeSegment newSegment = cubeInstance.getSegments(SegmentStatusEnum.NEW).get(0);
        Assert.assertTrue(newSegment != null);
        final List<CubeSegment> mergingSegments = cubeInstance.getMergingSegments(newSegment);

        Assert.assertTrue(mergingSegments != null && mergingSegments.size() > 0);

        File folder = new File("../examples/test_case_data/localmeta/secondary_index");
        System.out.println(folder.getAbsolutePath());
        SegmentIndexMerge segmentIndexMerge = new SegmentIndexMerge(newSegment, mergingSegments, folder);

        List<File> files = segmentIndexMerge.mergeIndex();
        Assert.assertTrue(files != null && files.size() > 0);

        FileUtils.forceDelete(new File(folder, newSegment.getName()));
    }
}