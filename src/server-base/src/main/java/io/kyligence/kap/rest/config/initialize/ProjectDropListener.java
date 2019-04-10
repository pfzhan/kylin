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

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;

import io.kyligence.kap.common.persistence.transaction.EventListenerRegistry;
import io.kyligence.kap.event.manager.EventOrchestratorManager;
import io.kyligence.kap.rest.service.NFavoriteScheduler;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectDropListener implements EventListenerRegistry.ResourceEventListener {

    @Override
    public void onUpdate(KylinConfig config, RawResource rawResource) {
        // override for default
    }

    @Override
    public void onDelete(KylinConfig config, String resPath) {
        val term = resPath.split("\\/");
        if (!resPath.startsWith(ResourceStore.PROJECT_ROOT))
            return;

        val project = term[3];
        log.debug("delete project {}", project);

        val kylinConfig = KylinConfig.getInstanceFromEnv();

        try {
            deleteStorage(config, project.split("\\.")[0]);
            EventOrchestratorManager.getInstance(kylinConfig).shutdownByProject(project);
            NFavoriteScheduler.shutdownByProject(project);
            NDefaultScheduler.shutdownByProject(project);
        } catch (IOException e) {
            log.warn("error when delete " + project + " storage", e);
        }

    }

    private void deleteStorage(KylinConfig config, String project) throws IOException {
        String strPath = config.getHdfsWorkingDirectory(project);
        FileSystem fs = HadoopUtil.getFileSystem(strPath);
        if (fs.exists(new Path(strPath))) {
            fs.delete(new Path(strPath), true);
        }
    }

}
