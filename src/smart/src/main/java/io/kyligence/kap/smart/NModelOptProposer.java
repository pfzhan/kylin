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

package io.kyligence.kap.smart;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.project.ProjectInstance;

import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.smart.NSmartContext.NModelContext;
import io.kyligence.kap.smart.common.AccelerateInfo;
import io.kyligence.kap.smart.model.NModelMaster;

class NModelOptProposer extends NAbstractProposer {

    NModelOptProposer(NSmartContext smartContext) {
        super(smartContext);
    }

    @Override
    void propose() {
        if (smartContext.getModelContexts() == null)
            return;

        final ProjectInstance projectInstance = NProjectManager.getInstance(kylinConfig).getProject(project);
        if (projectInstance.isExpertMode()) {
            logger.info("Expert mode doesn't support modify an existing model.");
            return;
        }

        for (NSmartContext.NModelContext modelCtx : smartContext.getModelContexts()) {
            NModelMaster modelMaster = new NModelMaster(modelCtx);
            NDataModel model = modelCtx.getTargetModel();
            if (model == null) {
                model = modelMaster.proposeInitialModel();
                logger.info("Initialized a new model({}) for no compatible one to use.", model.getId());
            }

            try {
                model = modelMaster.proposeJoins(model);
                model = modelMaster.proposePartition(model);
                model = modelMaster.proposeComputedColumn(model);
                model = modelMaster.proposeScope(model);
                modelCtx.setTargetModel(model);
            } catch (Exception e) {
                logger.error("Unexpected exception occurs in initialize target model.", e);
                modelCtx.setTargetModel(null);
                recordException(modelCtx, e);
            }
        }

        reduceSnapshotModel();
    }

    private void reduceSnapshotModel() {
        List<NModelContext> snapshotModelCandidate = smartContext.getModelContexts().stream()
                .filter(ctx -> ctx.getOriginModel() == null && ctx.getTargetModel() != null
                        && CollectionUtils.isEmpty(ctx.getTargetModel().getJoinTables())
                        && CollectionUtils.isEmpty(ctx.getTargetModel().getComputedColumnDescs())
                        && !ctx.getTargetModel().getRootFactTable().getTableDesc().isIncrementLoading())
                .collect(Collectors.toList());

        NDataflowManager manager = NDataflowManager.getInstance(smartContext.getKylinConfig(),
                smartContext.getProject());
        Set<String> snapshotProviders = manager.listAllDataflows().stream().filter(NDataflow::isReady)
                .map(NDataflow::getLatestReadySegment).filter(Objects::nonNull).map(seg -> seg.getSnapshots().keySet())
                .flatMap(Collection::stream).collect(Collectors.toSet());
        snapshotModelCandidate.stream().forEach(modelContext -> {
            String rootTable = modelContext.getTargetModel().getRootFactTable().getTableIdentity();
            if (snapshotProviders.contains(rootTable)) {
                final Map<String, AccelerateInfo> sql2AccelerateInfo = smartContext.getAccelerateInfoMap();
                // TODO add snapshot to AccelerateInfo
                modelContext.setTargetModel(null);
            }
        });
    }

}
