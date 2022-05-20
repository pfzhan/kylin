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

package io.kyligence.kap.rest.delegate;

import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.rest.request.AddSegmentRequest;
import io.kyligence.kap.rest.request.DataFlowUpdateRequest;
import io.kyligence.kap.rest.request.MergeSegmentRequest;
import io.kyligence.kap.rest.request.ModelRequest;
import io.kyligence.kap.rest.response.BuildBaseIndexResponse;
import io.kyligence.kap.rest.service.ModelService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kylin.metadata.model.Segments;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.util.SpringContext;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Set;

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
}
