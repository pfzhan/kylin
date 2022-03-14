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

package io.kyligence.kap.newten.auto;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.MeasureDesc;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.util.ExecAndComp.CompareLevel;
import lombok.val;

public class AutoReproposeValidationTest extends AutoTestBase {

    @Test
    public void testReproposeSQLWontChangeOriginMetadata() throws Exception {
        val folder = "sql_for_automodeling/repropose";
        try {
            // 1. create metadata
            proposeWithSmartMaster(getProject(), Lists.newArrayList(new TestScenario(CompareLevel.SAME, folder)));
            buildAllModels(KylinConfig.getInstanceFromEnv(), getProject());

            // 2. ensure metadata
            List<MeasureDesc> measureDescs = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .listEffectiveRewriteMeasures(getProject(), "EDW.TEST_SITES");
            measureDescs.stream().filter(measureDesc -> measureDesc.getFunction().isSum()).forEach(measureDesc -> {
                FunctionDesc func = measureDesc.getFunction();
                Assert.assertFalse(func.getColRefs().isEmpty());
            });
        } finally {
            FileUtils.deleteQuietly(new File("../kap-it/metastore_db"));
        }
    }

}
