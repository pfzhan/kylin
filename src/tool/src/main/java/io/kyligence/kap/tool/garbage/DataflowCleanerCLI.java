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
package io.kyligence.kap.tool.garbage;

import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.project.ProjectInstance;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflow;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataflowCleanerCLI {

    public static void main(String[] args) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(config);
        for (ProjectInstance project : projectManager.listAllProjects()) {
            log.info("Start dataflow cleanup for project<{}>", project.getName());
            try {
                cleanupRedundantIndex(project);
            } catch (Exception e) {
                log.warn("clean dataflow for project<" + project.getName() + "> failed", e);
            }
            log.info("Dataflow cleanup for project<{}> finished", project.getName());
        }
    }

    private static void cleanupRedundantIndex(ProjectInstance project) {
        UnitOfWork.doInTransactionWithRetry(() -> {
            val models = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project.getName())
                    .listUnderliningDataModels();
            for (NDataModel model : models) {
                removeLayouts(model);
            }
            return 0;
        }, project.getName());
    }

    private static void removeLayouts(NDataModel model) {
        val dataflowManager = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), model.getProject());
        val dataflow = dataflowManager.getDataflow(model.getUuid());
        val layoutIds = getLayouts(dataflow);
        val toBeRemoved = Sets.<Long> newHashSet();
        for (NDataSegment segment : dataflow.getSegments()) {
            toBeRemoved.addAll(segment.getSegDetails().getLayouts().stream().map(NDataLayout::getLayoutId)
                    .filter(id -> !layoutIds.contains(id)).collect(Collectors.toSet()));
        }
        dataflowManager.removeLayouts(dataflow, Lists.newArrayList(toBeRemoved));
    }

    private static List<Long> getLayouts(NDataflow dataflow) {
        val cube = dataflow.getIndexPlan();
        val layouts = cube.getAllLayouts();
        return layouts.stream().map(LayoutEntity::getId).collect(Collectors.toList());
    }
}
