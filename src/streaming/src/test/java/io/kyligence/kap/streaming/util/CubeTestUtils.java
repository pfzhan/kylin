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
package io.kyligence.kap.streaming.util;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import org.apache.kylin.common.KylinConfig;

public class CubeTestUtils {
    public static void createTmpModel(KylinConfig config, IndexPlan indexPlan, String project, String modelId) {
        val modelMgr = NDataModelManager.getInstance(config, project);
        val model = modelMgr.copyForWrite(modelMgr.getDataModelDesc(modelId));
        model.setUuid(indexPlan.getUuid());
        model.setAlias("tmp" + indexPlan.getUuid());
        model.setMvcc(-1);
        modelMgr.createDataModelDesc(model, null);

    }

    public static void createTmpModelAndCube(KylinConfig config, IndexPlan indexPlan, String project, String modelId) {
        createTmpModel(config, indexPlan, project, modelId);
        val indePlanManager = NIndexPlanManager.getInstance(config, project);
        if (indePlanManager.getIndexPlan(indexPlan.getUuid()) == null) {
            indexPlan.setMvcc(-1);
            indePlanManager.createIndexPlan(indexPlan);
        }
    }
}
