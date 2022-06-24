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

package io.kyligence.kap.job.execution;

import java.time.Clock;
import java.time.LocalDate;
import java.util.Locale;

import org.apache.kylin.common.util.BasicEmailNotificationContent;
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.job.constant.JobIssueEnum;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EmailNotificationContent extends BasicEmailNotificationContent {

    private AbstractExecutable executable;

    public static EmailNotificationContent createContent(JobIssueEnum issue, AbstractExecutable executable) {
        EmailNotificationContent content = new EmailNotificationContent();
        content.setIssue(issue.getDisplayName());
        content.setTime(LocalDate.now(Clock.systemDefaultZone()).toString());
        content.setJobType(executable.getJobType().toString());
        content.setProject(executable.getProject());
        content.setExecutable(executable);
        switch (issue) {
        case JOB_ERROR:
            content.setConclusion(CONCLUSION_FOR_JOB_ERROR);
            content.setSolution(SOLUTION_FOR_JOB_ERROR);
            break;
        case LOAD_EMPTY_DATA:
            content.setConclusion(CONCLUSION_FOR_LOAD_EMPTY_DATA);
            content.setSolution(
                    SOLUTION_FOR_LOAD_EMPTY_DATA.replaceAll("\\$\\{model_name\\}", executable.getTargetModelAlias()));
            break;
        case SOURCE_RECORDS_CHANGE:
            content.setConclusion(CONCLUSION_FOR_SOURCE_RECORDS_CHANGE);
            content.setSolution(SOLUTION_FOR_SOURCE_RECORDS_CHANGE
                    .replaceAll("\\$\\{start_time\\}",
                            DateFormat.formatToDateStr(executable.getDataRangeStart(),
                                    DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS))
                    .replaceAll("\\$\\{end_time\\}", DateFormat.formatToDateStr(executable.getDataRangeEnd(),
                            DateFormat.DEFAULT_DATETIME_PATTERN_WITHOUT_MILLISECONDS)));
            break;
        default:
            throw new IllegalArgumentException(String.format(Locale.ROOT, "no process for jobIssue: %s.", issue));
        }
        return content;
    }
}
