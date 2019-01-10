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

package io.kyligence.kap.event.handle;

import com.google.common.collect.Sets;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.engine.spark.job.NSparkCubingJob;
import lombok.var;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;

import java.lang.reflect.InvocationTargetException;
import java.util.UUID;

public class PostAddSegmentHandlerTest extends NLocalFileMetadataTestCase {

    @Before
    public void setUp() {
        this.createTestMetadata();
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testMarkModelOnline_WhenStoppedIncJobExist() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        val project = "default";
        val dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        var dataflow = dataflowManager.getDataflow(dataflowId);
        val executableManager = NExecutableManager.getInstance(getTestConfig(), project);

        val job = NSparkCubingJob.create(Sets.newHashSet(dataflow.getSegments()),
                Sets.newLinkedHashSet(dataflow.getIndexPlan().getAllLayouts()), "", JobTypeEnum.INC_BUILD, UUID.randomUUID().toString());
        executableManager.addJob(job);
        // pause IncJob will mark dataflow status to LAG_BEHIND
        executableManager.pauseJob(job.getId());

        testMarkDFOnlineIfNecessary();
        dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(dataflowId);
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, dataflow.getStatus());
    }

    @Test
    public void testMarkModelOnline_WhenDataLoadingRangeCannotCover() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        val project = "default";
        val dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        dataflowManager.updateDataflow(dataflowId, copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND);
        });
        NDataflowUpdate update = new NDataflowUpdate(dataflowId);
        update.setToRemoveSegs(dataflowManager.getDataflow(dataflowId).getSegments().toArray(new NDataSegment[0]));
        dataflowManager.updateDataflow(update);
        dataflowManager.appendSegment(dataflowManager.getDataflow(dataflowId), SegmentRange.TimePartitionedSegmentRange.createInfinite());


        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), project);
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        dataLoadingRange.setCoveredRange(SegmentRange.TimePartitionedSegmentRange.createInfinite());
        dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        testMarkDFOnlineIfNecessary();
        val dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(dataflowId);
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, dataflow.getStatus());
    }

    @Test
    public void testMarkModelOnline_WhenNoErrorJObAndDataLoadingRangeCover() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        val project = "default";
        val dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        var dataflowManager = NDataflowManager.getInstance(getTestConfig(), project);
        dataflowManager.updateDataflow(dataflowId, copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.LAG_BEHIND);
        });

        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), project);
        String tableName = "DEFAULT.TEST_KYLIN_FACT";
        String columnName = "TEST_KYLIN_FACT.CAL_DT";
        NDataLoadingRange dataLoadingRange = new NDataLoadingRange();
        dataLoadingRange.updateRandomUuid();
        dataLoadingRange.setTableName(tableName);
        dataLoadingRange.setColumnName(columnName);
        dataLoadingRange.setCoveredRange(SegmentRange.TimePartitionedSegmentRange.createInfinite());
        dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        testMarkDFOnlineIfNecessary();
        val dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(dataflowId);
        Assert.assertEquals(RealizationStatusEnum.ONLINE, dataflow.getStatus());
    }

    private void testMarkDFOnlineIfNecessary() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        val project = "default";
        val dataflowId = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        val postAddSegmentHandler = new PostAddSegmentHandler();
        val method = postAddSegmentHandler.getClass().getDeclaredMethod("markDFOnlineIfNecessary", NDataflow.class);
        method.setAccessible(true);
        var dataflow = NDataflowManager.getInstance(getTestConfig(), project).getDataflow(dataflowId);
        method.invoke(postAddSegmentHandler, dataflow);
    }

}
