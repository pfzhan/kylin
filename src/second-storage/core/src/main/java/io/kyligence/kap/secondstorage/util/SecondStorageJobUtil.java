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

package io.kyligence.kap.secondstorage.util;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.JobErrorCode;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.execution.JobTypeEnum;

import com.google.common.collect.Sets;

import io.kyligence.kap.job.execution.AbstractExecutable;
import io.kyligence.kap.job.manager.ExecutableManager;
import io.kyligence.kap.secondstorage.SecondStorageUtil;

public class SecondStorageJobUtil {
    public static final Set<JobTypeEnum> SECOND_STORAGE_JOBS = Sets.newHashSet(JobTypeEnum.EXPORT_TO_SECOND_STORAGE,
            JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN, JobTypeEnum.SECOND_STORAGE_MODEL_CLEAN);

    public static List<AbstractExecutable> findSecondStorageJobByProject(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        ExecutableManager executableManager = ExecutableManager.getInstance(config, project);
        return executableManager.getJobs().stream().map(executableManager::getJob)
                .filter(job -> SECOND_STORAGE_JOBS.contains(job.getJobType()))
                .collect(Collectors.toList());
    }

    public static void validateSegment(String project, String modelId, List<String> segmentIds) {
        List<AbstractExecutable> jobs = findSecondStorageJobByProject(project).stream()
                .filter(job -> SecondStorageUtil.RUNNING_STATE.contains(job.getStatusInMem()))
                .filter(job -> Objects.equals(job.getTargetModelId(), modelId))
                .filter(job -> {
                    List<String> targetSegments = Optional.ofNullable(job.getTargetSegments()).orElse(Collections.emptyList());
                    return targetSegments.stream().anyMatch(segmentIds::contains);
                })
                .collect(Collectors.toList());
        if (!jobs.isEmpty()) {
            throw new KylinException(JobErrorCode.SECOND_STORAGE_JOB_EXISTS,
                    MsgPicker.getMsg().getSecondStorageConcurrentOperate());
        }
    }

    public static void validateModel(String project, String modelId) {
        List<AbstractExecutable> jobs = findSecondStorageJobByProject(project).stream()
                .filter(job -> SecondStorageUtil.RUNNING_STATE.contains(job.getStatusInMem()))
                .filter(job -> Objects.equals(job.getTargetModelId(), modelId))
                .collect(Collectors.toList());
        if (!jobs.isEmpty()) {
            throw new KylinException(JobErrorCode.SECOND_STORAGE_JOB_EXISTS,
                    MsgPicker.getMsg().getSecondStorageConcurrentOperate());
        }
    }
}
