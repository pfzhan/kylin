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

package io.kyligence.kap.job.rest;

import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import com.google.common.base.Preconditions;
import io.kyligence.kap.rest.delegate.ModelMetadataInvoker;
import io.kyligence.kap.rest.delegate.TableMetadataInvoker;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.job.constant.JobTimeFilterEnum;

import static org.apache.kylin.common.exception.ServerErrorCode.INVALID_PARAMETER;

@Getter
@Setter
@AllArgsConstructor
public class JobFilter {

    private List<String> statuses;

    private List<String> jobNames;

    private int timeFilter;

    private String subject;

    private String key;

    private String project;

    private String sortBy;

    private boolean reverse;

    /**
     * for mybatis query
     * @return JobMapperFilter
     */
    public JobMapperFilter getJobMapperFilter(ModelMetadataInvoker modelMetadataInvoker,
            TableMetadataInvoker tableMetadataInvoker, int offset, int limit) {
        Date queryStartTime = getQueryStartTime(timeFilter);

        List<String> subjects = new ArrayList<>();
        if (StringUtils.isNotEmpty(subject)) {
            subjects.add(subject.trim());
        }
        // transform 'key' to subjects
        if (StringUtils.isNotEmpty(key)) {
            subjects.addAll(modelMetadataInvoker.getModelNamesByFuzzyName(key, project));
            subjects.addAll(tableMetadataInvoker.getTableNamesByFuzzyKey(project, key));
        }

        // if 'key' can not be transformed to 'subjects', then fuzzy query job id by 'key'
        String jobId = null;
        if (StringUtils.isNotEmpty(key) && subjects.isEmpty()) {
            jobId = "%" + key + "%";
        }

        String orderByField = convertSortBy(sortBy);

        String orderType = "ASC";
        if (reverse) {
            orderType = "DESC";
        }

        return new JobMapperFilter(statuses, jobNames, queryStartTime, subjects, null, jobId, null, project,
                orderByField, orderType, offset, limit);
    }

    private Date getQueryStartTime(int timeFilter) {
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
            throw new KylinException(INVALID_PARAMETER, msg.getILLEGAL_TIME_FILTER());
        }
    }

    private String convertSortBy(String sortBy) {
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
            throw new KylinException(INVALID_PARAMETER, msg.getJOB_SORT_BY_ERROR());
        }
    }
}
