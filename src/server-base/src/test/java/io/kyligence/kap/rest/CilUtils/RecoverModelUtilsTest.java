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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.kyligence.kap.rest.CilUtils;

import com.google.common.collect.Lists;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRange;
import io.kyligence.kap.metadata.cube.model.NDataLoadingRangeManager;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.AddCuboidEvent;
import io.kyligence.kap.event.model.AddSegmentEvent;
import io.kyligence.kap.event.model.PostAddCuboidEvent;
import io.kyligence.kap.event.model.PostAddSegmentEvent;
import io.kyligence.kap.metadata.model.ManagementType;
import io.kyligence.kap.metadata.model.NDataModelManager;

import io.kyligence.kap.rest.cli.RecoverModelUtil;
import lombok.val;
import lombok.var;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


import java.util.List;
import java.util.UUID;


public class RecoverModelUtilsTest extends NLocalFileMetadataTestCase {


    @Before
    public void setupResource() {
        System.setProperty("HADOOP_USER_NAME", "root");
        createTestMetadata();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testBroken_ModelBased_Model_OneNewSeg() {
        //recover by self
        val config = KylinConfig.getInstanceFromEnv();
        val eventDao = EventDao.getInstance(config, "default");
        val dfManager = NDataflowManager.getInstance(config, "default");
        var df = dfManager.getDataflowByModelAlias("nmodel_basic");

        val modelManager = NDataModelManager.getInstance(config, "default");
        val model = modelManager.getDataModelDescByAlias("nmodel_basic");

        modelManager.updateDataModel(model.getId(), copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });

        dfManager.updateDataflow(df.getId(), copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.BROKEN);
            copyForWrite.setSegments(new Segments<>());
        });
        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        //one new seg
        dfManager.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());

        df = dfManager.getDataflowByModelAlias("nmodel_basic");

        eventDao.deleteAllEvents();
        RecoverModelUtil.recoverBySelf(df, config, "default");

        val events = eventDao.getEventsOrdered();
        Assert.assertEquals(4, events.size());
        Assert.assertTrue(events.get(0) instanceof AddSegmentEvent);

        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());

    }

    @Test
    public void testBroken_ModelBased_Model_HasReadySeg() {
        //recover by self
        val config = KylinConfig.getInstanceFromEnv();
        val eventDao = EventDao.getInstance(config, "default");
        val dfManager = NDataflowManager.getInstance(config, "default");
        val modelManager = NDataModelManager.getInstance(config, "default");
        val model = modelManager.getDataModelDescByAlias("nmodel_basic");
        modelManager.updateDataModel(model.getId(), copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.MODEL_BASED);
        });

        var df = dfManager.getDataflowByModelAlias("nmodel_basic");
        dfManager.updateDataflow(df.getId(), copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.BROKEN);
            copyForWrite.setSegments(new Segments<>());
        });
        val update = new NDataflowUpdate(df.getId());
        List<NDataSegment> segs = Lists.newArrayList();
        // 1 ready seg,but cuboid is empty
        val seg1 = new NDataSegment();
        seg1.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(0L, 100L));
        seg1.setStatus(SegmentStatusEnum.READY);
        seg1.setId(UUID.randomUUID().toString());
        segs.add(seg1);

        //1 new seg
        val seg2 = new NDataSegment();
        seg2.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(100L, 200L));
        seg2.setStatus(SegmentStatusEnum.NEW);
        seg2.setId(UUID.randomUUID().toString());
        segs.add(seg2);

        update.setToAddSegs(segs.toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        df = dfManager.getDataflowByModelAlias("nmodel_basic");

        RecoverModelUtil.recoverBySelf(df, config, "default");

        val events = eventDao.getEventsOrdered();
        //addcuboid event, addsegment event
        Assert.assertEquals(4, events.size());
        Assert.assertTrue(events.get(0) instanceof AddSegmentEvent || events.get(0) instanceof PostAddSegmentEvent);

        Assert.assertTrue(events.get(3) instanceof AddCuboidEvent || events.get(3) instanceof PostAddCuboidEvent);
        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.ONLINE, df.getStatus());
    }

    @Test
    public void testBrokenTableOrientedModel_NoUsefulSeg() {
        val config = KylinConfig.getInstanceFromEnv();
        val eventDao = EventDao.getInstance(config, "default");
        val dfManager = NDataflowManager.getInstance(config, "default");
        val modelManager = NDataModelManager.getInstance(config, "default");
        val loadingRangeManager = NDataLoadingRangeManager.getInstance(config, "default");
        val loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        loadingRange.setColumnName("CAL_DT");
        //2012-01-03 2012-01-04
        loadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(1325520000000L, 1325606400000L));
        loadingRangeManager.createDataLoadingRange(loadingRange);
        var df = dfManager.getDataflowByModelAlias("nmodel_basic");
        dfManager.updateDataflow(df.getId(), copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.BROKEN);
            copyForWrite.setSegments(new Segments<>());
        });

        modelManager.updateDataModel(df.getId(), copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.TABLE_ORIENTED);
        });

        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        //a full build seg
        dfManager.appendSegment(df, SegmentRange.TimePartitionedSegmentRange.createInfinite());

        df = dfManager.getDataflowByModelAlias("nmodel_basic");

        eventDao.deleteAllEvents();

        RecoverModelUtil.recoverByDataloadingRange(df, config, "default");

        val events = eventDao.getEventsOrdered();
        Assert.assertEquals(4, events.size());
        Assert.assertTrue(events.get(0) instanceof AddSegmentEvent);
        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, df.getStatus());
    }


    @Test
    public void testBrokenTableOrientedModel_hasUsefulSeg() {
        val config = KylinConfig.getInstanceFromEnv();
        val eventDao = EventDao.getInstance(config, "default");
        val dfManager = NDataflowManager.getInstance(config, "default");
        val modelManager = NDataModelManager.getInstance(config, "default");
        val loadingRangeManager = NDataLoadingRangeManager.getInstance(config, "default");
        val loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        loadingRange.setColumnName("CAL_DT");
        //2012-01-03 2012-02-09  splited to 3 ranges
        loadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(DateFormat.stringToMillis("2012-01-03 00:00:00"), DateFormat.stringToMillis("2012-02-09 13:44:26")));
        loadingRangeManager.createDataLoadingRange(loadingRange);
        var df = dfManager.getDataflowByModelAlias("nmodel_basic");
        dfManager.updateDataflow(df.getId(), copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.BROKEN);
            copyForWrite.setSegments(new Segments<>());
        });

        modelManager.updateDataModel(df.getId(), copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.TABLE_ORIENTED);
        });

        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        val update = new NDataflowUpdate(df.getId());
        List<NDataSegment> segs = Lists.newArrayList();
        // 1 ready seg is useful,but cuboid is empty
        val seg1 = new NDataSegment();
        //1/03-2/01
        seg1.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(DateFormat.stringToMillis("2012-01-03 00:00:00"), DateFormat.stringToMillis("2012-02-01 00:00:00")));
        seg1.setStatus(SegmentStatusEnum.READY);
        seg1.setId(UUID.randomUUID().toString());
        segs.add(seg1);

        update.setToAddSegs(segs.toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        df = dfManager.getDataflowByModelAlias("nmodel_basic");

        eventDao.deleteAllEvents();

        RecoverModelUtil.recoverByDataloadingRange(df, config, "default");

        val events = eventDao.getEventsOrdered();
        //2 addsegmentevent, 1 addcuboid event
        Assert.assertEquals(6, events.size());
        Assert.assertTrue(events.get(0) instanceof AddSegmentEvent);
        Assert.assertTrue(events.get(2) instanceof AddSegmentEvent);
        Assert.assertTrue(events.get(4) instanceof AddCuboidEvent);
        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, df.getStatus());

    }

    @Test
    public void testBrokenTableOrientedModel_WithGap() {
        val config = KylinConfig.getInstanceFromEnv();
        val eventDao = EventDao.getInstance(config, "default");
        val dfManager = NDataflowManager.getInstance(config, "default");
        val modelManager = NDataModelManager.getInstance(config, "default");
        val loadingRangeManager = NDataLoadingRangeManager.getInstance(config, "default");
        val loadingRange = new NDataLoadingRange();
        loadingRange.setTableName("DEFAULT.TEST_KYLIN_FACT");
        loadingRange.setColumnName("CAL_DT");
        //2012-01-03 2012-02-09  splited to 3 ranges
        loadingRange.setCoveredRange(new SegmentRange.TimePartitionedSegmentRange(DateFormat.stringToMillis("2012-01-03 00:00:00"), DateFormat.stringToMillis("2012-02-09 13:44:26")));
        loadingRangeManager.createDataLoadingRange(loadingRange);
        var df = dfManager.getDataflowByModelAlias("nmodel_basic");
        dfManager.updateDataflow(df.getId(), copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.BROKEN);
            copyForWrite.setSegments(new Segments<>());
        });

        modelManager.updateDataModel(df.getId(), copyForWrite -> {
            copyForWrite.setManagementType(ManagementType.TABLE_ORIENTED);
        });

        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        val update = new NDataflowUpdate(df.getId());
        List<NDataSegment> segs = Lists.newArrayList();
        // 1 ready seg is useful,but cuboid is empty
        val seg1 = new NDataSegment();
        //1/03-2/01
        seg1.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(DateFormat.stringToMillis("2012-01-03 00:00:00"), DateFormat.stringToMillis("2012-02-01 00:00:00")));
        seg1.setStatus(SegmentStatusEnum.READY);
        seg1.setId(UUID.randomUUID().toString());
        segs.add(seg1);

        val seg2 = new NDataSegment();
        //2/06-02/09
        seg2.setSegmentRange(new SegmentRange.TimePartitionedSegmentRange(DateFormat.stringToMillis("2012-02-06 00:00:00"), DateFormat.stringToMillis("2012-02-09 13:44:26")));
        seg2.setStatus(SegmentStatusEnum.READY);
        seg2.setId(UUID.randomUUID().toString());
        segs.add(seg2);

        update.setToAddSegs(segs.toArray(new NDataSegment[0]));
        dfManager.updateDataflow(update);

        df = dfManager.getDataflowByModelAlias("nmodel_basic");

        eventDao.deleteAllEvents();

        RecoverModelUtil.recoverByDataloadingRange(df, config, "default");

        val events = eventDao.getEventsOrdered();
        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        //1 addsegmentevent, 1 addcuboid event
        Assert.assertEquals(4, events.size());
        Assert.assertTrue(events.get(0) instanceof AddSegmentEvent);
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-01 00:00:00") + "", df.getSegment(((AddSegmentEvent) events.get(0)).getSegmentId()).getSegRange().getStart().toString());
        Assert.assertEquals(DateFormat.stringToMillis("2012-02-06 00:00:00") + "", df.getSegment(((AddSegmentEvent) events.get(0)).getSegmentId()).getSegRange().getEnd().toString());

        Assert.assertTrue(events.get(2) instanceof AddCuboidEvent);

        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, df.getStatus());

    }


    @Test
    public void testBrokenTableOrientedModel_NoDataloadingRange() {
        val config = KylinConfig.getInstanceFromEnv();
        val eventDao = EventDao.getInstance(config, "default");
        val dfManager = NDataflowManager.getInstance(config, "default");

        var df = dfManager.getDataflowByModelAlias("nmodel_basic");
        dfManager.updateDataflow(df.getId(), copyForWrite -> {
            copyForWrite.setStatus(RealizationStatusEnum.BROKEN);
            copyForWrite.setSegments(new Segments<>());
        });


        df = dfManager.getDataflowByModelAlias("nmodel_basic");

        eventDao.deleteAllEvents();

        RecoverModelUtil.recoverByDataloadingRange(df, config, "default");

        val events = eventDao.getEventsOrdered();
        //1 addsegmentevent
        Assert.assertEquals(4, events.size());
        Assert.assertTrue(events.get(0) instanceof AddSegmentEvent);
        df = dfManager.getDataflowByModelAlias("nmodel_basic");
        Assert.assertEquals(RealizationStatusEnum.LAG_BEHIND, df.getStatus());
        Assert.assertTrue(df.getSegments().get(0).getSegRange().isInfinite());

    }

}
