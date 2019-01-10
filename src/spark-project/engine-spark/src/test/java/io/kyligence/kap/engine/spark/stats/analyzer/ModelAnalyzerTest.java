/*
 * Copyright (C) 2016 Kyligence Inc. All rights reserved.
 *
 * http://kyligence.io
 *
 * This software is the confidential and proprietary information of
 * Kyligence Inc. ("Confidential Information"). You shall not disclose
 * such Confidential Information and shall use it only in accordance
 * with the terms of the license agreement you entered into with
 * Kyligence Inc.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package io.kyligence.kap.engine.spark.stats.analyzer;

import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import org.apache.kylin.measure.hllc.HLLCSerializer;
import org.apache.kylin.measure.hllc.HLLCounter;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class ModelAnalyzerTest extends NLocalWithSparkSessionTest {

    private static final HLLCSerializer HLLC_SERIALIZER = new HLLCSerializer(DataType.getType("hllc14"));

    private NDataModel dataModel;

    @Before
    public void setup() {
        dataModel = NDataModelManager.getInstance(getTestConfig(), "default").getDataModelDesc("89af4ee2-2cdb-4b07-b39e-4c29856309aa");
    }

    @Test
    public void testAnalyze() throws IOException {
        final NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(getTestConfig(),
                "default");

        final List<SegmentRange> segmentRanges = new ArrayList<>();

        final ModelAnalyzer modelAnalyzer = new ModelAnalyzer(dataModel, getTestConfig());
        TableExtDesc tableExtDesc = tableMetadataManager.getOrCreateTableExt(dataModel.getRootFactTableName());
        assertTableExtDescEquals(tableExtDesc, 0L, 0, segmentRanges);

        // analysis round 1
        NDataSegment segment = buildDataSegment(segmentRanges, 1325347200000L, 1326124800000L);
        PartitionDesc partitionDesc = dataModel.getPartitionDesc();
        partitionDesc.setPartitionDateFormat("yyyy-MM-dd");
        dataModel.setPartitionDesc(partitionDesc);
        modelAnalyzer.analyze(segment, ss);
        tableExtDesc = tableMetadataManager.getOrCreateTableExt(dataModel.getRootFactTableName());
        assertTableExtDescEquals(tableExtDesc, 101L, 11, segmentRanges);
        assertColStatsEquals(tableExtDesc.getColumnStats(1), 1, segment.getSegRange(), 98L, 98L, 0, 4988.0d, 19.0d, 4,
                2, "4709", "19");

        // analysis round 2
        segment = buildDataSegment(segmentRanges, 1326124800000L, 1326988800000L);
        modelAnalyzer.analyze(segment, ss);
        tableExtDesc = tableMetadataManager.getOrCreateTableExt(dataModel.getRootFactTableName());
        assertTableExtDescEquals(tableExtDesc, 251L, 11, segmentRanges);
        assertColStatsEquals(tableExtDesc.getColumnStats(1), 2, segment.getSegRange(), 147L, 241L, 0, 4997.0d, 4.0d, 4,
                1, "4709", "4");

        // analysis round 3
        segment = buildDataSegment(segmentRanges, 1326988800000L, 1327852800000L);
        modelAnalyzer.analyze(segment, ss);
        tableExtDesc = tableMetadataManager.getOrCreateTableExt(dataModel.getRootFactTableName());
        assertTableExtDescEquals(tableExtDesc, 377L, 11, segmentRanges);
        assertColStatsEquals(tableExtDesc.getColumnStats(1), 3, segment.getSegRange(), 123L, 356L, 0, 4997.0d, 4.0d, 4,
                1, "4709", "4");

        // analysis round 4, Repeat the round 2's segment range
        segment = buildDataSegment(segmentRanges, 1326124800000L, 1326988800000L);
        modelAnalyzer.analyze(segment, ss);
        tableExtDesc = tableMetadataManager.getOrCreateTableExt(dataModel.getRootFactTableName());
        assertTableExtDescEquals(tableExtDesc, 377L, 11, segmentRanges);
        assertColStatsEquals(tableExtDesc.getColumnStats(1), 3, segment.getSegRange(), 147L, 356L, 0, 4997.0d, 4.0d, 4,
                1, "4709", "4");

        // TODO test case: force update lookup table

    }

    private NDataSegment buildDataSegment(List<SegmentRange> segmentRanges, long start, long end) {
        final SegmentRange segmentRange = new SegmentRange.TimePartitionedSegmentRange(start, end);
        if (!segmentRanges.contains(segmentRange)) {
            segmentRanges.add(segmentRange);
        }
        final NDataSegment segment = new NDataSegment();
        segment.setSegmentRange(segmentRange);

        return segment;
    }

    private void assertTableExtDescEquals(TableExtDesc tableExtDesc, long totalRows, int colStatsSize,
            List<SegmentRange> segmentRanges) {
        Assert.assertEquals(colStatsSize, tableExtDesc.getColumnStats().size());
        Assert.assertEquals(totalRows, tableExtDesc.getTotalRows());
        Assert.assertEquals(segmentRanges, tableExtDesc.getLoadingRange());
    }

    private void assertColStatsEquals(TableExtDesc.ColumnStats colStats, int rangeSize, SegmentRange segmentRange,
            long rangeHllc, long totalCardinality, long nullCount, double maxNumeral, double minNumeral, int maxLength,
            int minLength, String maxLengthValue, String minLengthValue) {

        Assert.assertNotNull(colStats);
        Assert.assertEquals(rangeSize, colStats.getRangeHLLC().size());
        Assert.assertEquals(rangeHllc, getHLLC(colStats, segmentRange).getCountEstimate());
        Assert.assertEquals(totalCardinality, colStats.getTotalCardinality());
        Assert.assertEquals(nullCount, colStats.getNullCount());
        Assert.assertEquals(maxNumeral, colStats.getMaxNumeral(), 0.0001);
        Assert.assertEquals(minNumeral, colStats.getMinNumeral(), 0.0001);
        Assert.assertEquals(maxLength, colStats.getMaxLength().intValue());
        Assert.assertEquals(minLength, colStats.getMinLength().intValue());
        Assert.assertEquals(maxLengthValue, colStats.getMaxLengthValue());
        Assert.assertEquals(minLengthValue, colStats.getMinLengthValue());
    }

    private HLLCounter getHLLC(TableExtDesc.ColumnStats colStats, SegmentRange segmentRange) {
        return HLLC_SERIALIZER.deserialize(
                ByteBuffer.wrap(colStats.getRangeHLLC().get(segmentRange.getStart() + "," + segmentRange.getEnd())));
    }
}
