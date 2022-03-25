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
package org.apache.kylin.job;

import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.job.model.JobParam;

import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.kylin.job.common.ExecutableUtil.registerImplementation;

public class SecondStorageJobParamUtil {

    static {
        registerImplementation(JobTypeEnum.EXPORT_TO_SECOND_STORAGE, new SecondStorageJobUtil());
        registerImplementation(JobTypeEnum.SECOND_STORAGE_MODEL_CLEAN, new SecondStorageCleanJobUtil());
        registerImplementation(JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN, new SecondStorageCleanJobUtil());
        registerImplementation(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN, new SecondStorageCleanJobUtil());
        registerImplementation(JobTypeEnum.SECOND_STORAGE_INDEX_CLEAN, new SecondStorageCleanJobUtil());
    }

    private SecondStorageJobParamUtil() {
        throw new IllegalStateException("Utility class");
    }

    public static JobParam of(String project, String model, String owner, Stream<String> segmentIDs) {
        final JobParam param = new JobParam(model, owner);
        param.setProject(project);
        param.withTargetSegments(segmentIDs.collect(Collectors.toSet()))
                .withJobTypeEnum(JobTypeEnum.EXPORT_TO_SECOND_STORAGE);
        param.getCondition().put(JobParam.ConditionConstant.REFRESH_ALL_LAYOUTS, Boolean.FALSE);
        return param;
    }

    public static JobParam projectCleanParam(String project, String owner) {
        JobParam param = new JobParam("", owner);
        param.setJobTypeEnum(JobTypeEnum.SECOND_STORAGE_NODE_CLEAN);
        param.setProject(project);
        return param;
    }

    public static JobParam modelCleanParam(String project, String model, String owner) {
        JobParam param = new JobParam(model, owner);
        param.setProject(project);
        param.setJobTypeEnum(JobTypeEnum.SECOND_STORAGE_MODEL_CLEAN);
        return param;
    }

    public static JobParam segmentCleanParam(String project, String model, String owner, Set<String> ids) {
        JobParam param = new JobParam(model, owner);
        param.setProject(project);
        param.withTargetSegments(ids);
        param.setJobTypeEnum(JobTypeEnum.SECOND_STORAGE_SEGMENT_CLEAN);
        return param;
    }

    /**
     * build delete layout table parameters
     *
     * PRD_KE-34597 add index clean job
     *
     * @param project project name
     * @param model model id
     * @param owner owner
     * @param needDeleteLayoutIds required delete ids of layout
     * @return job parameters
     */
    public static JobParam layoutCleanParam(String project, String model, String owner, Set<Long> needDeleteLayoutIds,
                                            Set<String> segmentIds) {
        JobParam param = new JobParam(model, owner);
        param.setProject(project);
        param.withTargetSegments(segmentIds);
        param.setSecondStorageDeleteLayoutIds(needDeleteLayoutIds);
        param.setJobTypeEnum(JobTypeEnum.SECOND_STORAGE_INDEX_CLEAN);
        return param;
    }
}
