package io.kyligence.kap.storage.hbase;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.gridtable.GTScanRange;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.filter.LogicalTupleFilter;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

public class IndexGTScanRangePlannerTest extends LocalFileMetadataTestCase {

    private static final Logger logger = LoggerFactory.getLogger(IndexGTScanRangePlannerTest.class);

    CubeInstance cubeInstance;
    CubeSegment cubeSegment;
    File localIdxFolder;

    private IndexGTScanRangePlanner planner;

    private CubeManager mgr() {
        return CubeManager.getInstance(getTestConfig());
    }

    @Before
    public void setup() throws Exception {
        logger.info("setup");
        createTestMetadata();

        cubeInstance = mgr().getCube("test_kylin_cube_without_slr_secondary_index_2");
        cubeSegment = cubeInstance.getSegmentById("31c2e8a3-32ed-4550-9d92-db076b747b18");
        localIdxFolder = new File("../examples/test_case_data/localmeta/secondary_index/19700101000000_20121113000000");
        cubeSegment.setIndexPath("file://" + localIdxFolder.getAbsolutePath());
    }

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testPlanScanRanges() throws IOException {
        Cuboid cuboid = Cuboid.getBaseCuboid(cubeSegment.getCubeDesc());
        Set<TblColRef> dimensions = cubeSegment.getCubeDesc().listDimensionColumnsIncludingDerived();
        Set<TblColRef> groupbyDims = Sets.newHashSet();
        Collection<FunctionDesc> metrics = Lists.newArrayList();

        TupleFilter filter = new LogicalTupleFilter(TupleFilter.FilterOperatorEnum.OR);
        CompareTupleFilter filter1 = new CompareTupleFilter(TupleFilter.FilterOperatorEnum.IN);
        TblColRef column1 = cuboid.getColumns().get(0);
        Dictionary dict = cubeSegment.getDictionary(column1);
        filter1.addChild(new ColumnTupleFilter(column1));
        filter1.addChild(new ConstantTupleFilter("2012-01-01"));
        filter1.addChild(new ConstantTupleFilter("2012-01-02"));
        filter1.addChild(new ConstantTupleFilter("2012-01-05"));

        filter.addChild(filter1);

        planner = new IndexGTScanRangePlanner(cubeSegment, cuboid, filter, dimensions, groupbyDims, metrics);

        List<GTScanRange> ranges = planner.planScanRanges();
        System.out.println(ranges);
        assertEquals(2, ranges.size());
    }
}
