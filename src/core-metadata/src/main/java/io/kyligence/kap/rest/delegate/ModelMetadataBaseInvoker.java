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

import java.util.Set;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.apache.kylin.rest.util.SpringContext;

import io.kyligence.kap.common.persistence.metadata.HDFSMetadataStore;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.rest.request.DataFlowUpdateRequest;
import io.kyligence.kap.rest.service.ModelMetadataBaseService;

public class ModelMetadataBaseInvoker {

    public static ModelMetadataBaseInvoker getInstance() {
        MetadataStore metadataStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getMetadataStore();
        if (metadataStore instanceof HDFSMetadataStore) {
            throw new KylinRuntimeException("This request cannot be route to metadata server");
        }
        if (SpringContext.getApplicationContext() == null) {
            // for UT
            return new ModelMetadataBaseInvoker();
        } else {
            return SpringContext.getBean(ModelMetadataBaseInvoker.class);
        }
    }

    private final ModelMetadataBaseService modelMetadataBaseService = new ModelMetadataBaseService();

    public String getModelNameById(String modelId, String project) {
        return modelMetadataBaseService.getModelNameById(modelId, project);
    }

    public void updateIndex(String project, long epochId, String modelId, Set<Long> toBeDeletedLayoutIds,
                            boolean deleteAuto, boolean deleteManual) {
        modelMetadataBaseService.updateIndex(project, epochId, modelId, toBeDeletedLayoutIds, deleteAuto, deleteManual);
    }

    public void updateDataflow(DataFlowUpdateRequest dataFlowUpdateRequest) {
        modelMetadataBaseService.updateDataflow(dataFlowUpdateRequest);
    }

    public void updateDataflow(String project, String dfId, String segmentId, long maxBucketId) {
        modelMetadataBaseService.updateDataflow(project, dfId, segmentId, maxBucketId);
    }

    public void updateIndexPlan(String project, String uuid, IndexPlan indexplan, String action) {
        modelMetadataBaseService.updateIndexPlan(project, uuid, indexplan, action);
    }

    public void updateDataflowStatus(String project, String uuid, RealizationStatusEnum status) {
        modelMetadataBaseService.updateDataflowStatus(project, uuid, status);
    }
}
