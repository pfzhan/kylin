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
package org.apache.kylin.rest.service;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.LayoutEntity;
import org.apache.kylin.metadata.cube.model.NIndexPlanManager;
import org.apache.kylin.metadata.model.NDataModel;
import org.apache.kylin.rest.request.CreateBaseIndexRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;

import io.kyligence.kap.secondstorage.SecondStorageUpdater;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import lombok.Setter;

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

        if (SecondStorageUtil.isModelEnable(project, modelId)
                && hasChange(preBaseTableLayout, updatedBaseTableLayout)) {
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
