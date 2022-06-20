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

package io.kyligence.kap.secondstorage.test.utils;

import java.util.List;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.guava20.shaded.common.base.Preconditions;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.metadata.NodeGroup;
import io.kyligence.kap.secondstorage.metadata.TableFlow;
import io.kyligence.kap.secondstorage.metadata.TablePlan;

public abstract class SecondStorageMetadataHelperTest {

    public abstract String getProject();

    public abstract String getModelId();

    protected KylinConfig getConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    protected TableFlow getTableFlow() {
        Preconditions.checkState(SecondStorageUtil.tableFlowManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(getModelId()).isPresent());
        return SecondStorageUtil.tableFlowManager(getConfig(), getProject()).get().get(getModelId()).get();
    }

    protected TablePlan getTablePlan() {
        Preconditions.checkState(SecondStorageUtil.tablePlanManager(getConfig(), getProject()).isPresent());
        Preconditions.checkState(SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(getModelId()).isPresent());
        return SecondStorageUtil.tablePlanManager(getConfig(), getProject()).get().get(getModelId()).get();
    }

    protected IndexPlan getIndexPlan() {
        return NIndexPlanManager.getInstance(getConfig(), getProject()).getIndexPlan(getModelId());
    }

    protected NDataflow getDataFlow() {
        return NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject()).getDataflow(getModelId());
    }

    protected NDataModelManager getNDataModelManager() {
        return NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    protected NDataModel getNDataModel() {
        return getNDataModelManager().getDataModelDesc(getModelId());
    }

    protected ExecutableManager getExecutableManager() {
        return ExecutableManager.getInstance(KylinConfig.getInstanceFromEnv(), getProject());
    }

    protected List<NodeGroup> getNodeGroups() {
        Preconditions.checkState(SecondStorageUtil.nodeGroupManager(getConfig(), getProject()).isPresent());
        return SecondStorageUtil.nodeGroupManager(getConfig(), getProject()).get().listAll();
    }
}
