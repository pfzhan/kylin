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

package io.kyligence.kap.smart.query;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RootPersistentEntity;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;

class NLocalQueryRunnerBuilder {

    private KylinConfig srcKylinConfig;
    private String[] sqls;
    private int nThreads;

    NLocalQueryRunnerBuilder(KylinConfig srcKylinConfig, String[] sqls, int nThreads) {
        this.srcKylinConfig = srcKylinConfig;
        this.sqls = sqls;
        this.nThreads = nThreads;
    }

    NLocalQueryRunner buildBasic(String projectName) {
        Set<String> dumpResources = Sets.newHashSet();
        Map<String, RootPersistentEntity> mockupResources = Maps.newHashMap();
        prepareResources(projectName, dumpResources, mockupResources, Lists.newArrayList(), true);
        return new NLocalQueryRunner(srcKylinConfig, projectName, sqls, dumpResources, mockupResources, nThreads);
    }

    NLocalQueryRunner buildPureMetaOnlyWithTables(String projectName) {
        Set<String> dumpResources = Sets.newHashSet();
        Map<String, RootPersistentEntity> mockupResources = Maps.newHashMap();
        prepareResources(projectName, dumpResources, mockupResources, Lists.newArrayList(), false);
        return new NLocalQueryRunner(srcKylinConfig, projectName, sqls, dumpResources, mockupResources, nThreads);
    }

    NLocalQueryRunner buildWithModelDescs(String projectName, List<NDataModel> modelDescs) {
        Set<String> dumpResources = Sets.newHashSet();
        Map<String, RootPersistentEntity> mockupResources = Maps.newHashMap();
        prepareResources(projectName, dumpResources, mockupResources, modelDescs, true);
        return new NLocalQueryRunner(srcKylinConfig, projectName, sqls, dumpResources, mockupResources, nThreads);
    }

    private void prepareResources(String projectName, Set<String> dumpResources,
            Map<String, RootPersistentEntity> mockupResources, List<NDataModel> dataModels, boolean containModel) {

        ProjectInstance dumpProj = new ProjectInstance();
        dumpProj.setName(projectName);
        dumpProj.setDefaultDatabase(
                NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).getDefaultDatabase(projectName));
        dumpProj.init(KylinConfig.getInstanceFromEnv());
        mockupResources.put(dumpProj.getResourcePath(), dumpProj);

        NTableMetadataManager metadataManager = NTableMetadataManager.getInstance(srcKylinConfig, projectName);
        metadataManager.listAllTables().forEach(tableDesc -> dumpResources.add(tableDesc.getResourcePath()));

        if (!containModel)
            return;

        dataModels.forEach(dataModel -> {
            mockupResources.put(dataModel.getResourcePath(), dataModel);
            // now get healthy model list through NDataflowManager.listUnderliningDataModels,
            // then here mockup the dataflow and indexPlan for the dataModel
            mockupDataflowAndIndexPlan(dataModel, projectName, mockupResources);
        });
    }

    private void mockupDataflowAndIndexPlan(NDataModel dataModel, String projectName,
            Map<String, RootPersistentEntity> mockupResources) {
        IndexPlan indexPlan = new IndexPlan();
        indexPlan.setUuid(dataModel.getUuid());
        indexPlan.setProject(projectName);
        indexPlan.setDescription(StringUtils.EMPTY);
        NDataflow dataflow = NDataflow.create(indexPlan);
        dataflow.setProject(projectName);
        mockupResources.put(indexPlan.getResourcePath(), indexPlan);
        mockupResources.put(dataflow.getResourcePath(), dataflow);
    }
}
