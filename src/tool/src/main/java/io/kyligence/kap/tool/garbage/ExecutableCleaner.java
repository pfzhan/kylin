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
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableState;
import org.apache.kylin.job.execution.NExecutableManager;

import io.kyligence.kap.event.manager.EventDao;
import io.kyligence.kap.event.model.JobRelatedEvent;

public class ExecutableCleaner implements MetadataCleaner {

    @Override
    public void cleanup(String project) {

        KylinConfig config = KylinConfig.getInstanceFromEnv();

        long expirationTime = config.getExecutableSurvivalTimeThreshold();

        NExecutableManager executableManager = NExecutableManager.getInstance(config, project);
        EventDao eventDao = EventDao.getInstance(config, project);

        List<AbstractExecutable> executables = executableManager.getAllExecutables();
        List<String> referencedJobIds = eventDao.getEvents().stream()
                .filter(event -> (event instanceof JobRelatedEvent)).map(event -> ((JobRelatedEvent) event).getJobId())
                .collect(Collectors.toList());
        List<AbstractExecutable> filteredExecutables = executables.stream().filter(job -> {
            if ((System.currentTimeMillis() - job.getCreateTime()) < expirationTime) {
                return false;
            }
            ExecutableState state = job.getStatus();
            if (!state.isFinalState()) {
                return false;
            }
            if (referencedJobIds.contains(job.getId())) {
                return false;
            }
            return true;
        }).collect(Collectors.toList());

        for (AbstractExecutable executable : filteredExecutables) {
            executableManager.deleteJob(executable.getId());
        }
    }
}
