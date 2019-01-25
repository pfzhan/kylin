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

package org.apache.kylin.job.execution;

import java.time.LocalDate;

import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.job.constant.JobIssueEnum;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class EmailNotificationContent {

    public static final String NOTIFY_EMAIL_TITLE_TEMPLATE = "[Kyligence System Notification] ${issue}";
    public static final String NOTIFY_EMAIL_BODY_TEMPLATE = "<div style='display:block;word-wrap:break-word;width:80%;font-size:16px;font-family:Microsoft YaHei;'><b>Dear Kyligence Customer,</b><pre><p>"
            + "<p>${conclusion}</p>" + "<p>Issue: ${issue}<br>" + "Type: ${type}<br>" + "Time: ${time}<br>"
            + "Project: ${project}<br>" + "Solution: ${solution}</p>" + "<p>Yours sincerely,<br>" + "Kyligence team</p>"
            + "</pre><div/>";

    public static final String CONCLUSION_FOR_JOB_ERROR = "We found an error job happened in your Kyligence system as below. It won't affect your system stability and you may repair it by following instructions.";
    public static final String CONCLUSION_FOR_LOAD_EMPTY_DATA = "We found a job has loaded empty data in your Kyligence system as below. It won't affect your system stability and you may reload data by following instructions.";
    public static final String CONCLUSION_FOR_SOURCE_RECORDS_CHANGE = "We found some source records updated in your Kyligence system. You can reload updated records by following instructions. Ignore this issue may cause query result inconsistency over different indexes.";

    public static final String SOLUTION_FOR_JOB_ERROR = "You may resume the job first. If still won't work, please send the job's diagnose package to kyligence technical support.";
    public static final String SOLUTION_FOR_LOAD_EMPTY_DATA = "You may refresh the empty segment of the model ${model_name} to reload data.";
    public static final String SOLUTION_FOR_SOURCE_RECORDS_CHANGE = "You may refresh the segment from ${start_time} to ${end_time} to apply source records change.";

    private String conclusion;
    private String issue;
    private String time;
    private String solution;
    private AbstractExecutable executable;

    public static EmailNotificationContent createContent(JobIssueEnum issue, AbstractExecutable executable) {
        EmailNotificationContent content = new EmailNotificationContent();
        content.setIssue(issue.getDisplayName());
        content.setTime(LocalDate.now().toString());
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
            throw new IllegalArgumentException(String.format("no process for jobIssue: %s.", issue));
        }
        return content;
    }

    public String getEmailTitle() {
        return NOTIFY_EMAIL_TITLE_TEMPLATE.replaceAll("\\$\\{issue\\}", issue);
    }

    public String getEmailBody() {
        return NOTIFY_EMAIL_BODY_TEMPLATE.replaceAll("\\$\\{conclusion\\}", conclusion)
                .replaceAll("\\$\\{issue\\}", issue).replaceAll("\\$\\{type\\}", executable.getJobType().toString())
                .replaceAll("\\$\\{time\\}", time).replaceAll("\\$\\{project\\}", executable.getProject())
                .replaceAll("\\$\\{solution\\}", solution);
    }

}
