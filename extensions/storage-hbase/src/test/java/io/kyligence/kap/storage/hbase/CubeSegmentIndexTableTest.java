package io.kyligence.kap.storage.hbase;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.ByteArray;
import org.apache.kylin.common.util.BytesUtil;
import org.apache.kylin.common.util.Dictionary;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeManager;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.cube.cuboid.Cuboid;
import org.apache.kylin.cube.gridtable.CubeGridTable;
import org.apache.kylin.metadata.filter.ColumnTupleFilter;
import org.apache.kylin.metadata.filter.CompareTupleFilter;
import org.apache.kylin.metadata.filter.ConstantTupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;
import io.kyligence.kap.cube.gridtable.GTScanRanges;
import io.kyligence.kap.cube.index.ColumnIndexWriter;

public class CubeSegmentIndexTableTest extends LocalFileMetadataTestCase {

    CubeInstance cubeInstance;
    CubeSegment cubeSegment;
    File localIdxFolder;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        cubeInstance = mgr().getCube("test_kylin_cube_without_slr_left_join_ready");
        localIdxFolder = new File("/tmp/kylin/index");
        FileUtils.forceMkdir(localIdxFolder);
        cubeSegment = cubeInstance.getSegments().get(0);
        cubeSegment.setIndexPath("file://" + localIdxFolder.getAbsolutePath());
    }

    @After
    public void after() throws Exception {
        cleanAfterClass();

        FileUtils.forceDelete(localIdxFolder);
    }

    private CubeManager mgr() {
        return CubeManager.getInstance(getTestConfig());
    }

    private void buildColumnIndex(TblColRef tblColRef) throws IOException {
        File fwdIdxFile = new File(localIdxFolder, tblColRef.getName() + ".fwd");
        File invIdxFile = new File(localIdxFolder, tblColRef.getName() + ".inv");

        Dictionary<String> dict = cubeSegment.getDictionary(tblColRef);
        if (dict == null)
            return;

        int maxValue = dict.getMaxId();
        int colLength = Integer.SIZE - Integer.numberOfLeadingZeros(maxValue);
        final int LENGTH_OF_TABLE = 1000;
        int[] colValues = new int[LENGTH_OF_TABLE];
        List<Integer> resultRows = Lists.newArrayList();

        // generate the data;
        resultRows.add(dict.getMinId());
        for (int i = 1; i < LENGTH_OF_TABLE; i++) {
            colValues[i] = i % maxValue;
            if (colValues[i] == colValues[0]) {
                resultRows.add(i);
            }
        }

        try (ColumnIndexWriter writer = new ColumnIndexWriter(tblColRef, dict, 0, colLength, fwdIdxFile, invIdxFile)) {
            for (int i = 0; i < LENGTH_OF_TABLE; i++) {
                byte[] row = new byte[colLength];
                BytesUtil.writeUnsigned(colValues[i], row, 0, colLength);
                writer.write(row);
            }
        }
    }

    private GTScanRanges lookupColumn(TblColRef tblColRef) throws IOException {
        CompareTupleFilter filter = new CompareTupleFilter(CompareTupleFilter.FilterOperatorEnum.GT);
        filter.addChild(new ColumnTupleFilter(tblColRef));

        Dictionary<String> dict = cubeSegment.getDictionary(tblColRef);

        byte[] row = new byte[dict.getSize()];
        BytesUtil.writeUnsigned(dict.getMinId() + 2, row, 0, dict.getSize());
        filter.addChild(new ConstantTupleFilter(new ByteArray(row)));

        CubeSegmentIndexTable indexTable = new CubeSegmentIndexTable(cubeSegment, CubeGridTable.newGTInfo(cubeSegment, Cuboid.getBaseCuboidId(cubeSegment.getCubeDesc())), Cuboid.getBaseCuboid(cubeSegment.getCubeDesc()));
        GTScanRanges ranges = indexTable.lookup(filter);

        return ranges;
    }

    @Test
    public void testLookup() throws IOException {
        for (TblColRef dictCol : cubeSegment.getCubeDesc().getAllColumnsHaveDictionary()) {
            buildColumnIndex(dictCol);
        }

        TblColRef tblColRef = cubeInstance.getDescriptor().getRowkey().getRowKeyColumns()[2].getColRef();
        GTScanRanges ranges = lookupColumn(tblColRef);
        assert ranges.getRangeSet().size() > 0;
    }
}
