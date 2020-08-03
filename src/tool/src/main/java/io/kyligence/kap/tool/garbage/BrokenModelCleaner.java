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

package io.kyligence.kap.tool.garbage;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BrokenModelCleaner implements MetadataCleaner {
    private static final Logger logger = LoggerFactory.getLogger(BrokenModelCleaner.class);
    @Override
    public void cleanup(String project) {
        logger.info("Start to clean broken model in project {}", project);
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        val dataModelManager = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), project);

        for (NDataModel model : dataflowManager.listUnderliningDataModels(true)) {
            val dataflow = dataflowManager.getDataflow(model.getId());
            if (dataflow.checkBrokenWithRelatedInfo()) {
                dataflowManager.dropDataflow(model.getId());
                indexPlanManager.dropIndexPlan(model.getId());
                dataModelManager.dropModel(dataModelManager.getDataModelDesc(model.getId()));
            }
        }
        logger.info("Clean broken model in project {} finished", project);

    }
}
