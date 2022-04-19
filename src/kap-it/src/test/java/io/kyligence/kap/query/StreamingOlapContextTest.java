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

package io.kyligence.kap.query;

import java.util.stream.Collectors;

import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.routing.RealizationChooser;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.engine.spark.NLocalWithSparkSessionTest;
import io.kyligence.kap.metadata.query.NativeQueryRealization;
import io.kyligence.kap.smart.SmartContext;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.util.AccelerationContextUtil;
import lombok.val;

public class StreamingOlapContextTest extends NLocalWithSparkSessionTest {

    @Before
    public void setUp() throws Exception {
        staticCreateTestMetadata("src/test/resources/ut_meta/streaming");
        getTestConfig().setProperty("kylin.streaming.enabled", "true");
    }

    @Override
    public String getProject() {
        return "streaming_test2";
    }

    @Test
    public void testHybridRealizationContext() {
        /**
         * hit both batch and streaming
         */
        String project = getProject();
        String sql = "select count(*) from SSB_STREAMING";
        val proposeContext = new SmartContext(getTestConfig(), project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context.olapSchema.setConfigOnlyInTest(getTestConfig().base());
        RealizationChooser.attemptSelectCandidate(context);
        OLAPContext.registerContext(context);

        val nativeQueryRealizations = OLAPContext.getNativeRealizations();
        Assert.assertEquals(nativeQueryRealizations.size(), 2);

        val modelIdAliasMap = nativeQueryRealizations.stream()
                .collect(Collectors.toMap(NativeQueryRealization::getModelId, NativeQueryRealization::getModelAlias));

        Assert.assertEquals(modelIdAliasMap.size(), 2);
        Assert.assertEquals(modelIdAliasMap.get("4965c827-fbb4-4ea1-a744-3f341a3b030d"), "model_streaming");
        Assert.assertEquals(modelIdAliasMap.get("cd2b9a23-699c-4699-b0dd-38c9412b3dfd"), "model_streaming");
    }

    @Test
    public void testOnlyBatchRealizationContextOnHybridModel() {
        /**
         * hit batch model only,
         * the fact table is hive table
         */
        String project = getProject();
        String sql = "select LO_ORDERKEY, count(*) from SSB.LINEORDER_HIVE group by LO_ORDERKEY";
        val proposeContext = new SmartContext(getTestConfig(), project, new String[] { sql });
        SmartMaster smartMaster = new SmartMaster(proposeContext);
        smartMaster.runUtWithContext(null);
        proposeContext.saveMetadata();
        AccelerationContextUtil.onlineModel(proposeContext);
        OLAPContext context = Lists
                .newArrayList(smartMaster.getContext().getModelContexts().get(0).getModelTree().getOlapContexts())
                .get(0);
        context.olapSchema.setConfigOnlyInTest(getTestConfig().base());
        RealizationChooser.attemptSelectCandidate(context);
        OLAPContext.registerContext(context);

        val nativeQueryRealizations = OLAPContext.getNativeRealizations();
        Assert.assertEquals(nativeQueryRealizations.size(), 1);

        val nativeQueryRealization = nativeQueryRealizations.get(0);

        Assert.assertEquals(nativeQueryRealization.getModelId(), "cd2b9a23-699c-4699-b0dd-38c9412b3dfd");
        Assert.assertEquals(nativeQueryRealization.getModelAlias(), "model_streaming");
    }
}
