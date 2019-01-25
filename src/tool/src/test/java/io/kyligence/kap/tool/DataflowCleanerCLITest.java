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

package io.kyligence.kap.tool;

import com.google.common.collect.Sets;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.tool.garbage.DataflowCleanerCLI;
import lombok.val;
import lombok.var;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.stream.Collectors;

public class DataflowCleanerCLITest extends NLocalFileMetadataTestCase {
    private static final String PROJECT = "default";
    private static final String MODEL_ID = "89af4ee2-2cdb-4b07-b39e-4c29856309aa";

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    @Test
    public void testDataflowCleaner() {
        val dataflowManager = NDataflowManager.getInstance(getTestConfig(), PROJECT);
        var dataflow = dataflowManager.getDataflow(MODEL_ID);
        val indexPlanManager = NIndexPlanManager.getInstance(getTestConfig(), PROJECT);
        var indexPlan = dataflow.getIndexPlan();
        indexPlanManager.updateIndexPlan(indexPlan.getId(), copyForWrite -> copyForWrite.removeLayouts(Sets.newHashSet(10001L, 10002L), LayoutEntity::equals, true,
                false));

        for (NDataSegment segment : dataflow.getSegments()) {
            var layouts = segment.getSegDetails().getLayouts().stream().map(NDataLayout::getLayoutId).collect(Collectors.toList());
            Assert.assertTrue(layouts.contains(10001L));
            Assert.assertTrue(layouts.contains(10002L));
        }

        DataflowCleanerCLI.main(new String[0]);

        dataflow = dataflowManager.getDataflow(MODEL_ID);
        for (NDataSegment segment : dataflow.getSegments()) {
            var layouts = segment.getSegDetails().getLayouts().stream().map(NDataLayout::getLayoutId).collect(Collectors.toList());
            Assert.assertFalse(layouts.contains(10001L));
            Assert.assertFalse(layouts.contains(10002L));
        }
    }

}
