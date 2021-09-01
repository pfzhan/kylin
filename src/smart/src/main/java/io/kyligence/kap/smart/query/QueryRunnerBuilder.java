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
import org.apache.kylin.metadata.realization.RealizationStatusEnum;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.streaming.KafkaConfigManager;

public class QueryRunnerBuilder {

    private final KylinConfig kylinConfig;
    private final String[] sqls;
    private final String project;
    private List<NDataModel> models = Lists.newArrayList();

    public QueryRunnerBuilder(String project, KylinConfig kylinConfig, String[] sqls) {
        this.kylinConfig = kylinConfig;
        this.sqls = sqls;
        this.project = project;
    }

    public QueryRunnerBuilder of(List<NDataModel> models) {
        this.models = models;
        return this;
    }

    public LocalQueryRunner build() {
        Set<String> dumpResources = Sets.newHashSet();
        Map<String, RootPersistentEntity> mockupResources = Maps.newHashMap();
        prepareResources(dumpResources, mockupResources, models);
        return new LocalQueryRunner(kylinConfig, project, sqls, dumpResources, mockupResources);
    }

    private void prepareResources(Set<String> dumpResources, Map<String, RootPersistentEntity> mockupResources,
            List<NDataModel> dataModels) {

        ProjectInstance dumpProj = new ProjectInstance();
        dumpProj.setName(project);
        dumpProj.setDefaultDatabase(NProjectManager.getInstance(kylinConfig).getDefaultDatabase(project));
        dumpProj.init(kylinConfig, true);
        mockupResources.put(dumpProj.getResourcePath(), dumpProj);

        NTableMetadataManager metadataManager = NTableMetadataManager.getInstance(kylinConfig, project);
        metadataManager.listAllTables().forEach(tableDesc -> dumpResources.add(tableDesc.getResourcePath()));

        KafkaConfigManager kafkaConfigManager = KafkaConfigManager.getInstance(kylinConfig, project);
        kafkaConfigManager.listAllKafkaConfigs().forEach(kafkaConfig -> dumpResources.add(kafkaConfig.getResourcePath()));

        dataModels.forEach(dataModel -> {
            mockupResources.put(dataModel.getResourcePath(), dataModel);
            // now get healthy model list through NDataflowManager.listUnderliningDataModels,
            // then here mockup the dataflow and indexPlan for the dataModel
            mockupDataflowAndIndexPlan(dataModel, mockupResources);
        });
    }

    private void mockupDataflowAndIndexPlan(NDataModel dataModel, Map<String, RootPersistentEntity> mockupResources) {
        IndexPlan indexPlan = new IndexPlan();
        indexPlan.setUuid(dataModel.getUuid());
        indexPlan.setProject(project);
        indexPlan.setDescription(StringUtils.EMPTY);
        NDataflow dataflow = NDataflow.create(indexPlan, RealizationStatusEnum.ONLINE);
        dataflow.setProject(project);
        mockupResources.put(indexPlan.getResourcePath(), indexPlan);
        mockupResources.put(dataflow.getResourcePath(), dataflow);
    }
}
