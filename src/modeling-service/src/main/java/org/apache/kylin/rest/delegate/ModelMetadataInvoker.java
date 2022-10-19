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

package org.apache.kylin.rest.delegate;

import java.util.List;
import java.util.Set;

import org.apache.kylin.metadata.cube.model.IndexPlan;
import org.apache.kylin.metadata.cube.model.NDataSegment;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.request.AddSegmentRequest;
import org.apache.kylin.rest.request.DataFlowUpdateRequest;
import org.apache.kylin.rest.request.MergeSegmentRequest;
import org.apache.kylin.rest.request.ModelRequest;
import org.apache.kylin.rest.response.BuildBaseIndexResponse;
import org.apache.kylin.rest.service.ModelService;
import org.apache.kylin.rest.util.SpringContext;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class ModelMetadataInvoker extends ModelMetadataBaseInvoker {
    private static ModelMetadataContract delegate = null;

    public static void setDelegate(ModelMetadataContract delegate) {
        if (ModelMetadataInvoker.delegate != null) {
            log.warn("Delegate is replaced as {}, origin value is {}", delegate, ModelMetadataInvoker.delegate);
        }
        ModelMetadataInvoker.delegate = delegate;
    }

    private ModelMetadataContract getDelegate() {
        if (delegate == null) {
            // Generally delegate will be set in ContractConfig, here is used for test
            return SpringContext.getBean(ModelService.class);
        }
        return delegate;
    }

    @Override
    public void updateIndex(String project, long epochId, String modelId, Set<Long> toBeDeletedLayoutIds,
                            boolean deleteAuto, boolean deleteManual) {
        getDelegate().updateIndex(project, epochId, modelId, toBeDeletedLayoutIds, deleteAuto, deleteManual);
    }

    @Override
    public void updateDataflow(DataFlowUpdateRequest dataFlowUpdateRequest) {
        getDelegate().updateDataflow(dataFlowUpdateRequest);
    }

    @Override
    public void updateDataflow(String project, String dfId, String segmentId, long maxBucketIt) {
        getDelegate().updateDataflow(project, dfId, segmentId, maxBucketIt);
    }

    @Override
    public void updateIndexPlan(String project, String uuid, IndexPlan indexplan, String action) {
        getDelegate().updateIndexPlan(project, uuid, indexplan, action);
    }

    @Override
    public void updateDataflowStatus(String project, String uuid, RealizationStatusEnum status) {
        getDelegate().updateDataflowStatus(project, uuid, status);
    }

    public String updateSecondStorageModel(String project, String modelId) {
        return getDelegate().updateSecondStorageModel(project, modelId);
    }

    public BuildBaseIndexResponse updateDataModelSemantic(String project, ModelRequest request) {
        return getDelegate().updateDataModelSemantic(project, request);
    }

    public void saveDateFormatIfNotExist(String project, String modelId, String format) {
        getDelegate().saveDateFormatIfNotExist(project, modelId, format);
    }

    public NDataSegment appendSegment(AddSegmentRequest request) {
        return getDelegate().appendSegment(request);
    }

    public NDataSegment refreshSegment(String project, String indexPlanUuid, String segmentId) {
        return getDelegate().refreshSegment(project, indexPlanUuid, segmentId);
    }

    public NDataSegment appendPartitions(String project, String dfId, String segId, List<String[]> partitionValues) {
        return getDelegate().appendPartitions(project, dfId, segId, partitionValues);
    }

    public NDataSegment mergeSegments(String project, MergeSegmentRequest mergeSegmentRequest) {
        return getDelegate().mergeSegments(project, mergeSegmentRequest);
    }

    public void purgeModelManually(String dataflowId, String project) {
        getDelegate().purgeModelManually(dataflowId, project);
    }

    public void deleteSegmentById(String model, String project, String[] ids, boolean force) {
        getDelegate().deleteSegmentById(model, project, ids, force);
    }

    public void removeIndexesFromSegments(String project, String modelId, List<String> segmentIds,
                                          List<Long> indexIds) {
        getDelegate().removeIndexesFromSegments(project, modelId, segmentIds, indexIds);
    }

    public List<String> getModelNamesByFuzzyName(String fuzzyName, String project) {
        return getDelegate().getModelNamesByFuzzyName(fuzzyName, project);
    }

    public String getModelNameById(String modelId, String project) {
        return getDelegate().getModelNameById(modelId, project);
    }

    public Segments<NDataSegment> getSegmentsByRange(String modelId, String project, String start, String end) {
        return getDelegate().getSegmentsByRange(modelId, project, start, end);
    }

    public void updateRecommendationsCount(String project, String modelId, int size) {
        getDelegate().updateRecommendationsCount(project, modelId, size);
    }
}
