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

package io.kyligence.kap.rest.config.initialize;

import java.util.Locale;

import org.apache.kylin.common.KylinConfig;

import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class ModelUpdateListener {

    @Subscribe
    public void onModelRename(NDataModel.ModelRenameEvent event) {
        String project = event.getProject();
        String modelId = event.getSubject();
        String newName = event.getNewName();
        String buildId = String.format(Locale.ROOT, "%s_%s", modelId, "build");
        String mergeId = String.format(Locale.ROOT, "%s_%s", modelId, "merge");
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            StreamingJobManager streamingJobManager = StreamingJobManager.getInstance(KylinConfig.getInstanceFromEnv(),
                    project);
            streamingJobManager.updateStreamingJob(buildId, copyForWrite -> copyForWrite.setModelName(newName));
            streamingJobManager.updateStreamingJob(mergeId, copyForWrite -> copyForWrite.setModelName(newName));
            return null;
        }, project);
    }
}
