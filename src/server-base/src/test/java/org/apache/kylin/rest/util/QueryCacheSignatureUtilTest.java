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

package org.apache.kylin.rest.util;

import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.SegmentStatusEnum;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.query.NativeQueryRealization;

public class QueryCacheSignatureUtilTest extends NLocalFileMetadataTestCase {
    private String project = "cache";
    private String modelId = "8c670664-8d05-466a-802f-83c023b56c77";
    private Long layoutId = 10001L;
    private SQLResponse response = new SQLResponse();
    private NDataflowManager dataflowManager;
    private NDataflow dataflow;

    @Before
    public void setup() throws Exception {
        this.createTestMetadata("src/test/resources/ut_cache");
        List<NativeQueryRealization> nativeRealizations = Lists
                .newArrayList(new NativeQueryRealization(modelId, layoutId, "TEST"));
        response.setNativeRealizations(nativeRealizations);
        response.setSignature(QueryCacheSignatureUtil.createCacheSignature(response, project));
        dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        dataflow = dataflowManager.getDataflow(modelId);
    }

    @After
    public void after() throws Exception {
        cleanupTestMetadata();
    }

    @Test
    public void testMultiRealizations() {
        SQLResponse response = new SQLResponse();
        List<NativeQueryRealization> multiRealizations = Lists.newArrayList(
                new NativeQueryRealization(modelId, layoutId, "TEST"),
                new NativeQueryRealization(modelId, 10002L, "TEST"));
        response.setNativeRealizations(multiRealizations);
        String cacheSignature = QueryCacheSignatureUtil.createCacheSignature(response, project);
        Assert.assertEquals("1538323200000_1538323200000", cacheSignature.split(",")[0].split(";")[0]);
        Assert.assertEquals("1538323300000_1538323300000", cacheSignature.split(",")[1].split(";")[0]);
    }

    @Test
    public void testMultiRealizationsWhenDeleteLayout() {
        SQLResponse response = new SQLResponse();
        List<NativeQueryRealization> multiRealizations = Lists.newArrayList(
                new NativeQueryRealization(modelId, layoutId, "TEST"),
                new NativeQueryRealization(modelId, 10002L, "TEST"));
        response.setNativeRealizations(multiRealizations);
        NDataflowUpdate update = new NDataflowUpdate(modelId);
        update.setToRemoveLayouts(dataflowManager.getDataflow(modelId).getFirstSegment().getLayout(10002L));
        update.setToRemoveLayouts(dataflowManager.getDataflow(modelId).getLastSegment().getLayout(10002L));
        dataflowManager.updateDataflow(update);
        response.setSignature(QueryCacheSignatureUtil.createCacheSignature(response, project));
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCreateCacheSignature() {
        Assert.assertEquals("1538323200000_1538323200000", response.getSignature().split(";")[0]);
    }

    @Test
    public void testCheckCacheExpiredWhenUpdateLayout() {
        NDataLayout layout = NDataLayout.newDataLayout(dataflow, dataflow.getSegments().getFirstSegment().getId(),
                layoutId);
        NDataflowUpdate update = new NDataflowUpdate(modelId);
        update.setToAddOrUpdateLayouts(layout);
        dataflowManager.updateDataflow(update);
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCheckCacheExpiredWhenUpdateOtherLayout() throws InterruptedException {
        Long otherLayout = 10002L;
        NDataLayout layout = NDataLayout.newDataLayout(dataflow, dataflow.getSegments().getFirstSegment().getId(),
                otherLayout);
        NDataflowUpdate update = new NDataflowUpdate(modelId);
        update.setToAddOrUpdateLayouts(layout);
        dataflowManager.updateDataflow(update);
        Thread.sleep(10L);
        Assert.assertFalse(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }

    @Test
    public void testCheckCacheExpiredWhenAddSegment() {
        SegmentRange.TimePartitionedSegmentRange timePartitionedSegmentRange = new SegmentRange.TimePartitionedSegmentRange(
                883612800000L, 1275321600000L);
        NDataSegment nDataSegment = dataflowManager.appendSegment(dataflowManager.getDataflow(modelId),
                timePartitionedSegmentRange);
        nDataSegment.setStatus(SegmentStatusEnum.READY);
        NDataLayout layout = NDataLayout.newDataLayout(dataflow, nDataSegment.getId(), layoutId);
        NDataflowUpdate update = new NDataflowUpdate(modelId);
        update.setToAddSegs(nDataSegment);
        update.setToAddOrUpdateLayouts(layout);
        dataflowManager.updateDataflow(update);
        Assert.assertTrue(QueryCacheSignatureUtil.checkCacheExpired(response, project));
    }
}
