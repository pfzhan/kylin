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
import org.apache.kylin.job.execution.ExecutableState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;


public class ExecutableCleaner extends MetadataCleaner {

    private static final Logger logger = LoggerFactory.getLogger(ExecutableCleaner.class);

    public ExecutableCleaner(String project) {
        super(project);
    }

    @Override
    public void cleanup() {

        logger.info("Start to clean executable in project {}", project);

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        long expirationTime = config.getExecutableSurvivalTimeThreshold();

        ExecutableManager executableManager = ExecutableManager.getInstance(config, project);

        List<AbstractExecutable> executables = executableManager.getAllExecutables();
        List<AbstractExecutable> filteredExecutables = executables.stream().filter(job -> {
            if ((System.currentTimeMillis() - job.getCreateTime()) < expirationTime) {
                return false;
            }
            ExecutableState state = job.getStatusInMem();
            if (!state.isFinalState()) {
                return false;
            }
            return true;
        }).collect(Collectors.toList());

        for (AbstractExecutable executable : filteredExecutables) {
            executableManager.deleteJob(executable.getId());
        }
        logger.info("Clean executable in project {} finished", project);
    }

}
