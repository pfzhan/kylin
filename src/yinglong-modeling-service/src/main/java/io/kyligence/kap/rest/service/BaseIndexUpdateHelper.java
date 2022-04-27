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

import lombok.Setter;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.util.SpringContext;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.rest.request.CreateBaseIndexRequest;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;

/**
 * due to complex model semantic update and table reload,
 * base index update work by record current info about baseindex, and after change(table reload..)
 * update base index according diff between previous info and current info.
 */
public class BaseIndexUpdateHelper {

    private long preBaseAggLayout;
    private long preBaseTableLayout;
    private static final long NON_EXIST_LAYOUT = -1;

    private String project;
    private String modelId;
    private boolean createIfNotExist;
    private boolean needUpdate;
    private boolean secondStorageEnabled = false;
    @Setter
    private boolean needCleanSecondStorage = true;

    public BaseIndexUpdateHelper(NDataModel model, boolean createIfNotExist) {

        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(),
                model.getProject());
        IndexPlan indexPlan = indexPlanManager.getIndexPlan(model.getId());
        if (!indexPlan.isBroken()) {
            needUpdate = indexPlan.getConfig().isBaseIndexAutoUpdate();
        }

        if (needUpdate) {
            project = model.getProject();
            modelId = model.getId();
            this.createIfNotExist = createIfNotExist;
            preBaseAggLayout = getBaseAggLayout();
            preBaseTableLayout = getBaseTableLayout();
        }
    }

    public BaseIndexUpdateHelper setSecondStorageEnabled(final boolean secondStorageEnabled) {
        this.secondStorageEnabled = secondStorageEnabled;
        return this;
    }

    public BuildBaseIndexResponse update(IndexPlanService service) {
        if (!needUpdate) {
            return BuildBaseIndexResponse.EMPTY;
        }
        if (notExist(preBaseAggLayout) && notExist(preBaseTableLayout) && !createIfNotExist) {
            return BuildBaseIndexResponse.EMPTY;
        }

        long curBaseTableLayout = getBaseTableLayout();
        boolean needCreateBaseTable = createIfNotExist;
        if (exist(preBaseTableLayout) && notExist(curBaseTableLayout)) {
            needCreateBaseTable = true;
        }

        Long curExistBaseAggLayout = getBaseAggLayout();
        boolean needCreateBaseAgg = createIfNotExist;
        if (exist(preBaseAggLayout) && notExist(curExistBaseAggLayout)) {
            needCreateBaseAgg = true;
        }
        if (secondStorageEnabled) {
            needCreateBaseAgg = false;
        }

        CreateBaseIndexRequest indexRequest = new CreateBaseIndexRequest();
        indexRequest.setModelId(modelId);
        indexRequest.setProject(project);
        BuildBaseIndexResponse response = service.updateBaseIndex(project, indexRequest, needCreateBaseAgg,
                needCreateBaseTable, true);
        response.judgeIndexOperateType(exist(preBaseAggLayout), true);
        response.judgeIndexOperateType(exist(preBaseTableLayout), false);

        long updatedBaseTableLayout = getBaseTableLayout();

        if (SecondStorageUtil.isModelEnable(project, modelId) && hasChange(preBaseTableLayout, updatedBaseTableLayout)) {
            SecondStorageUpdater updater = SpringContext.getBean(SecondStorageUpdater.class);
            updater.updateIndex(project, modelId);
        }
        return response;
    }

    private boolean notExist(long layout) {
        return layout == NON_EXIST_LAYOUT;
    }

    private boolean exist(long layout) {
        return layout != NON_EXIST_LAYOUT;
    }

    private boolean hasChange(long preId, long curId) {
        return preId != curId;
    }

    private long getBaseTableLayout() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        LayoutEntity layout = indexPlanManager.getIndexPlan(modelId).getBaseTableLayout();
        return layout != null ? layout.getId() : NON_EXIST_LAYOUT;
    }

    private long getBaseAggLayout() {
        NIndexPlanManager indexPlanManager = NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        LayoutEntity layout = indexPlanManager.getIndexPlan(modelId).getBaseAggLayout();
        return layout != null ? layout.getId() : NON_EXIST_LAYOUT;
    }

}
