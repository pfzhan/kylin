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
package io.kyligence.kap.rest.storage;

import java.util.List;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;

public class ModelCleaner implements GarbageCleaner {

    List<NDataModel> models = Lists.newArrayList();

    private boolean cleanBrokenOnly = false;

    public ModelCleaner() {
    }

    public ModelCleaner(boolean cleanBrokenOnly) {
        this.cleanBrokenOnly = cleanBrokenOnly;
    }

    @Override
    public void collect(NDataModel model) {
        val modelId = model.getId();
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflow = dataflowManager.getDataflow(modelId);
        if (cleanBrokenOnly && !dataflow.checkBrokenWithRelatedInfo()) {
            return;
        }
        models.add(model);
    }

    @Override
    public void cleanup() {
        for (NDataModel model : models) {
            val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
            val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
            val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
            dataflowManager.dropDataflow(model.getId());
            indexPlanManager.removeCubePlan(model.getId());
            dataModelManager.dropModel(model);
        }
    }
}
