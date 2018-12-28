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

package io.kyligence.kap.cube.model;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.SegmentConfig;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class NSegmentConfigHelper {

    final static ObjectMapper mapper = new ObjectMapper();

    public static SegmentConfig getModelSegmentConfig(String project, String model) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        NDataModel dataModel = NDataModelManager.getInstance(kylinConfig, project).getDataModelDesc(model);
        val segmentConfigInModel = dataModel.getSegmentConfig();
        val segmentConfigInProject = getFromProject(project, kylinConfig);
        val managementType = dataModel.getManagementType();
        switch (managementType) {
        case MODEL_BASED:
            return mergeConfig(segmentConfigInModel, segmentConfigInProject);
        case TABLE_ORIENTED:
            val segmentConfigInDataLoadingRange = getTableSegmentConfig(project, dataModel.getRootFactTableName());
            return mergeConfig(segmentConfigInModel, segmentConfigInDataLoadingRange);
        default:
            return null;
        }
    }

    public static SegmentConfig getTableSegmentConfig(String project, String table) {
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val segmentConfigInProject = getFromProject(project, kylinConfig);
        val segmentConfigInDataLoadingRange = getFromDataLoadingRange(project, kylinConfig, table);
        return mergeConfig(segmentConfigInDataLoadingRange, segmentConfigInProject);
    }

    private static SegmentConfig mergeConfig(SegmentConfig firstSegmentConfig, SegmentConfig secondSegmentConfig) {
        if (firstSegmentConfig == null && secondSegmentConfig == null) {
            return null;
        }
        if (firstSegmentConfig == null) {
            return secondSegmentConfig;
        }
        if (secondSegmentConfig == null) {
            return firstSegmentConfig;
        }
        Map<String, Object> firstSegmentConfigMap = mapper.convertValue(firstSegmentConfig, Map.class);
        Map<String, Object> secondSegmentConfigMap = mapper.convertValue(secondSegmentConfig, Map.class);
        secondSegmentConfigMap.entrySet().forEach(entry -> {
            val key = entry.getKey();
            val value = entry.getValue();
            if (firstSegmentConfigMap.get(key) == null) {
                firstSegmentConfigMap.put(key, value);
            }
        });
        return mapper.convertValue(firstSegmentConfigMap, SegmentConfig.class);
    }

    private static SegmentConfig getFromDataLoadingRange(String project, KylinConfig kylinConfig, String table) {
        val dataLoadingRangeManager = NDataLoadingRangeManager.getInstance(kylinConfig, project);
        val dataLoadingRange = dataLoadingRangeManager.getDataLoadingRange(table);
        if (dataLoadingRange == null) {
            return null;
        }
        return dataLoadingRange.getSegmentConfig();
    }

    private static SegmentConfig getFromProject(String project, KylinConfig kylinConfig) {
        val projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        Preconditions.checkState(projectInstance != null);
        return projectInstance.getSegmentConfig();
    }

}
