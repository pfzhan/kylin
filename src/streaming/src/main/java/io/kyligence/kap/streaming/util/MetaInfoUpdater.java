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

package io.kyligence.kap.streaming.util;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import io.kyligence.kap.metadata.cube.model.NDataLayout;
import io.kyligence.kap.metadata.cube.model.NDataSegment;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NDataflowUpdate;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.streaming.StreamingJobRecord;
import io.kyligence.kap.metadata.streaming.StreamingJobRecordManager;
import io.kyligence.kap.streaming.constants.StreamingConstants;
import io.kyligence.kap.streaming.manager.StreamingJobManager;
import lombok.val;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.job.constant.JobStatusEnum;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;
import java.util.Set;

public class MetaInfoUpdater {
    public static void update(String project, NDataSegment seg, NDataLayout layout) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NDataflowUpdate update = new NDataflowUpdate(seg.getDataflow().getUuid());
            update.setToAddOrUpdateLayouts(layout);
            NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateDataflow(update);
            return 0;
        }, project);
    }

    public static void updateJobState(String project, String jobId, JobStatusEnum state) {
        updateJobState(project, jobId, Sets.newHashSet(state), state);
    }

    public static void updateJobState(String project, String jobId, Set<JobStatusEnum> excludeStates,
            JobStatusEnum state) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            StreamingJobManager mgr = StreamingJobManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
            mgr.updateStreamingJob(jobId, copyForWrite -> {
                if (!excludeStates.contains(copyForWrite.getCurrentStatus())) {
                    copyForWrite.setCurrentStatus(state);
                    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss",
                            Locale.getDefault(Locale.Category.FORMAT));
                    Date date = new Date(System.currentTimeMillis());
                    copyForWrite.setLastUpdateTime(format.format(date));
                    val recordMgr = StreamingJobRecordManager.getInstance();
                    val record = new StreamingJobRecord();
                    record.setJobId(jobId);
                    record.setProject(project);
                    record.setCreateTime(System.currentTimeMillis());
                    switch (state) {
                    case STARTING:
                        if (!StringUtils.isEmpty(copyForWrite.getYarnAppUrl())) {
                            copyForWrite.setYarnAppId(StringUtils.EMPTY);
                            copyForWrite.setYarnAppUrl(StringUtils.EMPTY);
                        }
                        break;
                    case RUNNING:
                        copyForWrite.setLastStartTime(format.format(date));
                        copyForWrite.setSkipListener(false);
                        copyForWrite.setAction(StreamingConstants.ACTION_START);
                        record.setAction("START");
                        recordMgr.insert(record);
                        break;
                    case STOPPED:
                        record.setAction("STOP");
                        copyForWrite.setLastEndTime(format.format(date));
                        recordMgr.insert(record);
                        break;
                    case ERROR:
                        copyForWrite.setLastEndTime(format.format(date));
                        record.setAction("ERROR");
                        recordMgr.insert(record);
                        break;
                    default:
                    }
                }
            });
            return null;
        }, project);
    }

    public static void markGracefulShutdown(String project, String uuid) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val config = KylinConfig.getInstanceFromEnv();
            val mgr = StreamingJobManager.getInstance(config, project);
            mgr.updateStreamingJob(uuid, copyForWrite -> {
                if (copyForWrite != null) {
                    copyForWrite.setAction(StreamingConstants.ACTION_GRACEFUL_SHUTDOWN);
                }
            });
            return null;
        }, project);
    }
}
