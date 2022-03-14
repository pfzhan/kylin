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
package io.kyligence.kap.newten;

import com.google.common.collect.Sets;
import io.kyligence.kap.engine.spark.IndexDataConstructor;
import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.util.ExecAndComp;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparderEnv;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class NBuildAndQuerySnapshotTest extends NLocalWithSparkSessionTest {

    private KylinConfig config;
    private NDataflowManager dsMgr;

    @Before
    public void setUp() throws Exception {
        super.init();
        config = KylinConfig.getInstanceFromEnv();
        dsMgr = NDataflowManager.getInstance(config, getProject());
        indexDataConstructor = new IndexDataConstructor(getProject());
    }

    @Test
    public void testBasic() throws Exception {
        String dataflowName = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        cleanUpSegmentsAndIndexPlan(dataflowName);

        // before build snapshot
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(config, getProject());
        Assert.assertNull(tableMetadataManager.getTableDesc("DEFAULT.TEST_COUNTRY").getLastSnapshotPath());

        // build
        populateSSWithCSVData(config, getProject(), SparderEnv.getSparkSession());
        buildCube(dataflowName, SegmentRange.dateToLong("2012-01-01"), SegmentRange.dateToLong("2012-02-01"));

        // after build
        String lastSnapshotPath = tableMetadataManager.getTableDesc("DEFAULT.TEST_COUNTRY").getLastSnapshotPath();
        Assert.assertNotNull(lastSnapshotPath);
        Dataset dataset = ExecAndComp.queryModelWithoutCompute(getProject(), "select NAME from TEST_COUNTRY");
        Assert.assertEquals(244, dataset.collectAsList().size());
    }

    private void cleanUpSegmentsAndIndexPlan(String dfName) {
        NIndexPlanManager ipMgr = NIndexPlanManager.getInstance(config, getProject());
        String cubeId = dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getIndexPlan().getUuid();
        IndexPlan cube = ipMgr.getIndexPlan(cubeId);
        Set<Long> tobeRemovedLayouts = cube.getAllLayouts().stream()
                .filter(layout -> layout.getId() != 10001L)
                .map(LayoutEntity::getId)
                .collect(Collectors.toSet());

        ipMgr.updateIndexPlan(dsMgr.getDataflow("89af4ee2-2cdb-4b07-b39e-4c29856309aa").getIndexPlan().getUuid(), copyForWrite -> {
            copyForWrite.removeLayouts(tobeRemovedLayouts, true, true);
        });

        NDataflow df = dsMgr.getDataflow(dfName);
        NDataflowUpdate update = new NDataflowUpdate(df.getUuid());
        update.setToRemoveSegs(df.getSegments().toArray(new NDataSegment[0]));
        dsMgr.updateDataflow(update);
    }

    private void buildCube(String dfName, long start, long end) throws Exception {
        NDataflow df = dsMgr.getDataflow(dfName);
        List<LayoutEntity> layouts = df.getIndexPlan().getAllLayouts();
        indexDataConstructor.buildIndex(dfName, new SegmentRange.TimePartitionedSegmentRange(start, end), Sets.newLinkedHashSet(layouts), true);
    }

}
