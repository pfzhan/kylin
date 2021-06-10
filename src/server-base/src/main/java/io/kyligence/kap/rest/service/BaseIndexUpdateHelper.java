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
package io.kyligence.kap.rest.service;

import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.request.CreateBaseIndexRequest;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.util.SpringContext;

/**
 * due to complex model semantic update and table reload,
 * base index update work by record current info about baseindex, and after change(table reload..)
 * update base index according diff between previous info and current info.
 */
public class BaseIndexUpdateHelper {

    private boolean preExistBaseAggLayout;
    private boolean preExistBaseTableLayout;
    private String project;
    private String modelId;
    private boolean createIfNotExist;
    private boolean needUpdate;

    public BaseIndexUpdateHelper(NDataModel model, boolean createIfNotExist) {

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                model.getProject());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(model.getId());
        if (indexPlan.isBroken()) {
            needUpdate = false;
        } else {
            needUpdate = indexPlan.getConfig().isUpdateBaseIndexAutoMode();
        }

        if (needUpdate) {
            project = model.getProject();
            modelId = model.getId();
            this.createIfNotExist = createIfNotExist;
            preExistBaseAggLayout = existBaseAggLayout();
            preExistBaseTableLayout = existTableLayout();
        }
    }

    public BuildBaseIndexResponse update(IndexPlanService service) {
        if (!needUpdate) {
            return BuildBaseIndexResponse.EMPTY;
        }
        if (!preExistBaseAggLayout && !preExistBaseTableLayout && !createIfNotExist) {
            return BuildBaseIndexResponse.EMPTY;
        }

        boolean curExistBaseTableLayout = existTableLayout();
        boolean needCreateBaseTable = createIfNotExist;
        if (preExistBaseTableLayout && !curExistBaseTableLayout) {
            needCreateBaseTable = true;
        }

        boolean curExistBaseAgg = existBaseAggLayout();
        boolean needCreateBaseAgg = createIfNotExist;
        if (preExistBaseAggLayout && !curExistBaseAgg) {
            needCreateBaseAgg = true;
        }

        CreateBaseIndexRequest indexRequest = new CreateBaseIndexRequest();
        indexRequest.setModelId(modelId);
        indexRequest.setProject(project);
        BuildBaseIndexResponse response = service.updateBaseIndex(project, indexRequest, needCreateBaseAgg,
                needCreateBaseTable, true);
        response.judgeIndexOperateType(preExistBaseAggLayout, true);
        response.judgeIndexOperateType(preExistBaseTableLayout, false);
        if (SecondStorageUtil.isModelEnable(project, modelId)) {
            SecondStorageUpdater updater = SpringContext.getBean(SecondStorageUpdater.class);
            updater.onUpdate(project, modelId);
        }
        return response;
    }

    private boolean existTableLayout() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        return indexPlanManager.getIndexPlan(modelId).containBaseTableLayout();
    }

    private boolean existBaseAggLayout() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        return indexPlanManager.getIndexPlan(modelId).containBaseAggLayout();
    }

}
