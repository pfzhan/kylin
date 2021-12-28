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
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.job.execution.NExecutableManager;
import org.apache.kylin.job.impl.threadpool.NDefaultScheduler;

import io.kyligence.kap.common.metrics.MetricsGroup;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.query.RDBMSQueryHistoryDAO;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecManager;
import io.kyligence.kap.rest.service.task.QueryHistoryTaskScheduler;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ProjectDropListener {

    public void onDelete(String project) {
        log.debug("delete project {}", project);

        val kylinConfig = KylinConfig.getInstanceFromEnv();

        try {
            NExecutableManager.getInstance(kylinConfig, project).destoryAllProcess();
            StreamingJobManager.getInstance(kylinConfig, project).destroyAllProcess();
            RDBMSQueryHistoryDAO.getInstance().dropProjectMeasurement(project);
            RawRecManager.getInstance(project).deleteByProject(project);
            QueryHistoryTaskScheduler.shutdownByProject(project);
            NDefaultScheduler.shutdownByProject(project);

            MetricsGroup.removeProjectMetrics(project);
            if (KylinConfig.getInstanceFromEnv().isPrometheusMetricsEnabled()) {
                MetricsRegistry.deletePrometheusProjectMetrics(project);
            }
            EpochManager epochManager = EpochManager.getInstance();
            epochManager.deleteEpoch(project);
            deleteStorage(kylinConfig, project.split("\\.")[0]);
        } catch (Exception e) {
            log.warn("error when delete " + project + " storage", e);
        }
    }

    private void deleteStorage(KylinConfig config, String project) throws IOException {
        String strPath = config.getHdfsWorkingDirectory(project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        if (fs.exists(new Path(strPath))) {
            fs.delete(new Path(strPath), true);
        }
    }

}
