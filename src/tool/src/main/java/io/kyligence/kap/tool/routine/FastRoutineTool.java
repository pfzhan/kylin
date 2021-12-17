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

package io.kyligence.kap.tool.routine;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.OptionsHelper;
import org.apache.kylin.metadata.project.ProjectInstance;

import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.tool.MaintainModeTool;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class FastRoutineTool extends RoutineTool {

    @Override
    protected void execute(OptionsHelper optionsHelper) throws Exception {
        if (printUsage(optionsHelper)) {
            return;
        }
        initOptionValues(optionsHelper);
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        List<ProjectInstance> instances = NProjectManager.getInstance(kylinConfig).listAllProjects();
        List<String> projectsToCleanup = Arrays.asList(getProjects());
        if (projectsToCleanup.isEmpty()) {
            projectsToCleanup = instances.stream().map(ProjectInstance::getName).collect(Collectors.toList());
        }
        try {
            if (isMetadataCleanup()) {
                System.out.println("Start to fast cleanup metadata");
                MaintainModeTool maintainModeTool = new MaintainModeTool("fast routine tool");
                maintainModeTool.init();
                maintainModeTool.markEpochs();
                if (EpochManager.getInstance(kylinConfig).isMaintenanceMode()) {
                    Runtime.getRuntime().addShutdownHook(new Thread(maintainModeTool::releaseEpochs));
                }
                cleanMeta(projectsToCleanup);
            }
            System.out.println("Start to fast cleanup hdfs");
            cleanStorage();
        } catch (Exception e) {
            log.error("Failed to execute fast routintool", e);
        }
    }

    public static void main(String[] args) {
        FastRoutineTool tool = new FastRoutineTool();
        tool.execute(args);
        Unsafe.systemExit(0);
    }
}
