/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.job.util;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.config.JobMybatisConfig;
import org.apache.kylin.job.constant.JobTimeFilterEnum;
import org.apache.kylin.job.rest.JobFilter;
import org.apache.kylin.job.rest.JobMapperFilter;
import org.apache.kylin.rest.delegate.ModelMetadataInvoker;
import org.apache.kylin.rest.delegate.TableMetadataInvoker;

import com.google.common.base.Preconditions;

public class JobFilterUtil {

    public static JobMapperFilter getJobMapperFilter(final JobFilter jobFilter, int offset, int limit,
            ModelMetadataInvoker modelMetadataInvoker, TableMetadataInvoker tableMetadataInvoker) {
        Date queryStartTime = getQueryStartTime(jobFilter.getTimeFilter());

        List<String> subjects = new ArrayList<>();
        if (StringUtils.isNotEmpty(jobFilter.getSubject())) {
            subjects.add(jobFilter.getSubject().trim());
        }
        List<String> convertKeyToSubjects = new ArrayList<>();
        // transform 'key' to subjects
        if (StringUtils.isNotEmpty(jobFilter.getKey())) {
            convertKeyToSubjects
                    .addAll(modelMetadataInvoker.getModelNamesByFuzzyName(jobFilter.getKey(), jobFilter.getProject()));
            convertKeyToSubjects
                    .addAll(tableMetadataInvoker.getTableNamesByFuzzyKey(jobFilter.getProject(), jobFilter.getKey()));
            subjects.addAll(convertKeyToSubjects);
        }
        // if 'key' can not be transformed to 'subjects', then fuzzy query job id by 'key'
        String jobId = null;
        if (StringUtils.isNotEmpty(jobFilter.getKey()) && convertKeyToSubjects.isEmpty()) {
            jobId = "%" + jobFilter.getKey() + "%";
        }

        String orderByField = convertSortBy(jobFilter.getSortBy());

        String orderType = "ASC";
        if (jobFilter.isReverse()) {
            orderType = "DESC";
        }

        return new JobMapperFilter(jobFilter.getStatuses(), jobFilter.getJobNames(), queryStartTime, subjects, null,
                jobId, null, jobFilter.getProject(), orderByField, orderType, offset, limit,
                JobMybatisConfig.JOB_INFO_TABLE);
    }

    private static Date getQueryStartTime(int timeFilter) {
        JobTimeFilterEnum filterEnum = JobTimeFilterEnum.getByCode(timeFilter);
        Preconditions.checkNotNull(filterEnum, "Can not find the JobTimeFilterEnum by code: %s", timeFilter);

        // prepare time range
        Calendar calendar = Calendar.getInstance(TimeZone.getDefault(), Locale.getDefault(Locale.Category.FORMAT));
        calendar.setTime(new Date());
        Message msg = MsgPicker.getMsg();

        switch (filterEnum) {
        case LAST_ONE_DAY:
            calendar.add(Calendar.DAY_OF_MONTH, -1);
            return calendar.getTime();
        case LAST_ONE_WEEK:
            calendar.add(Calendar.WEEK_OF_MONTH, -1);
            return calendar.getTime();
        case LAST_ONE_MONTH:
            calendar.add(Calendar.MONTH, -1);
            return calendar.getTime();
        case LAST_ONE_YEAR:
            calendar.add(Calendar.YEAR, -1);
            return calendar.getTime();
        case ALL:
            return new Date(0);
        default:
            throw new KylinException(INVALID_PARAMETER, msg.getIllegalTimeFilter());
        }
    }

    private static String convertSortBy(String sortBy) {
        if (StringUtils.isEmpty(sortBy)) {
            return "update_time";
        }
        Message msg = MsgPicker.getMsg();
        switch (sortBy) {
        case "project":
        case "create_time":
            return sortBy;
        case "id":
            return "job_id";
        case "job_name":
            return "job_type";
        case "target_subject":
            return "model_id";
        case "total_duration":
            return "job_duration_millis";
        case "last_modified":
            return "update_time";
        default:
            throw new KylinException(INVALID_PARAMETER, msg.getJobSortByError());
        }
    }
}
