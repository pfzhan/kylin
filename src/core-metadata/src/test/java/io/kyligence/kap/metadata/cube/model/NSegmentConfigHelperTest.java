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

package io.kyligence.kap.metadata.cube.model;

import org.apache.kylin.metadata.model.SegmentConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.AutoMergeTimeEnum;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.RetentionRange;
import io.kyligence.kap.metadata.model.VolatileRange;
import lombok.val;
import lombok.var;

public class NSegmentConfigHelperTest extends NLocalFileMetadataTestCase {
    private String DEFAULT_PROJECT = "default";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void tearDown() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetSegmentConfig() {

        val model = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";
        // 1. MODEL_BASED && model segmentConfig is empty, get project segmentConfig
        val dataModelManager = NDataModelManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(getTestConfig(), DEFAULT_PROJECT);
        dataModelManager.updateDataModel(model, copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });
        SegmentConfig segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);

        Assert.assertEquals(true, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(3, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(0L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(false, segmentConfig.getRetentionRange().isRetentionRangeEnabled());

        // 2. MODEL_BASED && model segmentConfig is not empty, get mergedSegmentConfig of project segmentConfig and model SegmentConfig
        dataModelManager.updateDataModel(model, copyForWrite -> {
            copyForWrite
                    .setSegmentConfig(new SegmentConfig(false, Lists.newArrayList(AutoMergeTimeEnum.WEEK), null, null));
        });
        segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);
        Assert.assertEquals(false, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(1, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(0L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(false, segmentConfig.getRetentionRange().isRetentionRangeEnabled());

        // 3. TABLE_ORIENTED && model segmentConfig is empty, dataLoadingRange segmentConfig is empty, get project segmentConfig
        dataModelManager.updateDataModel(model, copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.TABLE_ORIENTED);
            copyForWrite.setSegmentConfig(new SegmentConfig());
        });

        var dataLoadingRange = new NDataLoadingRange();
        val dataModel = dataModelManager.getDataModelDesc(model);
        dataLoadingRange.setColumnName(dataModel.getPartitionDesc().getPartitionDateColumn());
        dataLoadingRange.setTableName(dataModel.getRootFactTableName());
        dataLoadingRange = dataLoadingRangeManager.createDataLoadingRange(dataLoadingRange);

        segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);
        Assert.assertEquals(true, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(3, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(0L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(false, segmentConfig.getRetentionRange().isRetentionRangeEnabled());

        // 4. TABLE_ORIENTED && model segmentConfig is empty, dataLoadingRange segmentConfig is not empty, get mergedSegmentConfig of project segmentConfig and dataLoadingRange SegmentConfig
        var copy = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
        copy.setSegmentConfig(new SegmentConfig(false, Lists.newArrayList(AutoMergeTimeEnum.WEEK), null, null));
        dataLoadingRange = dataLoadingRangeManager.updateDataLoadingRange(copy);

        segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);
        Assert.assertEquals(false, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(1, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(0L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(false, segmentConfig.getRetentionRange().isRetentionRangeEnabled());

        // 5. TABLE_ORIENTED && model segmentConfig is not empty, dataLoadingRange segmentConfig is not empty, get mergedSegmentConfig of project segmentConfig and dataLoadingRange SegmentConfig and model segmentConfig
        dataModelManager.updateDataModel(model, copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.TABLE_ORIENTED);
            copyForWrite
                    .setSegmentConfig(new SegmentConfig(false, Lists.newArrayList(AutoMergeTimeEnum.WEEK), null, null));
        });

        copy = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
        copy.setSegmentConfig(new SegmentConfig(null, null, new VolatileRange(1, true, AutoMergeTimeEnum.DAY),
                new RetentionRange(2, true, AutoMergeTimeEnum.DAY)));
        dataLoadingRange = dataLoadingRangeManager.updateDataLoadingRange(copy);

        segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);
        Assert.assertEquals(false, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(1, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(1L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(true, segmentConfig.getRetentionRange().isRetentionRangeEnabled());

        // 6. TABLE_ORIENTED && model segmentConfig is not empty, dataLoadingRange segmentConfig is empty, get mergedSegmentConfig of project segmentConfig and model segmentConfig
        copy = dataLoadingRangeManager.copyForWrite(dataLoadingRange);
        copy.setSegmentConfig(new SegmentConfig());
        dataLoadingRangeManager.updateDataLoadingRange(copy);

        segmentConfig = NSegmentConfigHelper.getModelSegmentConfig(DEFAULT_PROJECT, model);
        Assert.assertEquals(false, segmentConfig.getAutoMergeEnabled());
        Assert.assertEquals(1, segmentConfig.getAutoMergeTimeRanges().size());
        Assert.assertEquals(0L, segmentConfig.getVolatileRange().getVolatileRangeNumber());
        Assert.assertEquals(false, segmentConfig.getRetentionRange().isRetentionRangeEnabled());
    }

}
