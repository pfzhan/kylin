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

package org.apache.kylin.common.msg;

import java.util.List;
import java.util.Locale;

import org.apache.kylin.common.Singletons;

public class Message {

    protected Message() {

    }

    public static Message getInstance() {
        return Singletons.getInstance(Message.class);
    }

    // Cube
    public String getCHECK_CC_AMBIGUITY() {
        return "The computed column name \"%s\" has been used in the current model. Please rename it.";
    }

    public String getSEG_NOT_FOUND() {
        return "Can‘t find segment \"%s\" in model \"%s\". Please try again.";
    }

    public String getACL_INFO_NOT_FOUND() {
        return "Unable to find ACL information for object identity '%s'.";
    }

    public String getACL_DOMAIN_NOT_FOUND() {
        return "Can’t authorize at the moment due to unknown object. Please try again later, or contact technical support.";
    }

    public String getPARENT_ACL_NOT_FOUND() {
        return "Can’t authorize at the moment due to unknown object. Please try again later, or contact technical support.";
    }

    public String getIDENTITY_EXIST_CHILDREN() {
        return "Children exists for '%s'.";
    }

    // Model
    public String getINVALID_MODEL_DEFINITION() {
        return "Can’t find model. Please check and try again.";
    }

    public String getEMPTY_MODEL_NAME() {
        return "The model name can’t be empty.";
    }

    public String getINIT_MEASURE_FAILED() {
        return "Can’t initialize metadata at the moment. Please try restarting first. If the problem still exist, please contact technical support.";
    }

    public String getINVALID_MODEL_NAME() {
        return "The model name \"%s\" is invalid. Please use letters, numbers and underlines only.";
    }

    public String getINVALID_DIMENSION_NAME() {
        return "The dimension name \"%s\" is invalid. Please use only characters, numbers, spaces and symbol(_ -()%%?). %s characters at maximum are supported.";
    }

    public String getINVALID_MEASURE_NAME() {
        return "The measure name \"%s\" is invalid. Please use Chinese or English characters, numbers, spaces or symbol(_ -()%%?.). %s characters at maximum are supported.";
    }

    public String getDUPLICATE_DIMENSION_NAME() {
        return "Dimension name \"%s\" already exists. Please rename it. ";
    }

    public String getDUPLICATE_MEASURE_NAME() {
        return "Measure name \"%s\" already exists. Please rename it. ";
    }

    public String getDUPLICATE_MEASURE_DEFINITION() {
        return "The definition of this measure  is the same as measure \"%s\". Please modify it.";
    }

    public String getDUPLICATE_INTERNAL_MEASURE_DEFINITION() {
        return "The definition of this measure  is the same as internal measure \"%s\". Please modify it.";
    }

    public String getDUPLICATE_JOIN_CONDITIONS() {
        return "Can’t create the join condition between \"%s\" and \"%s\", because a same one already exists.";
    }

    public String getDUPLICATE_MODEL_NAME() {
        return "Model name '%s' is duplicated, could not be created.";
    }

    public String getBROKEN_MODEL_OPERATION_DENIED() {
        return "BROKEN model \"%s\" cannot be operated.";
    }

    public String getMODEL_NOT_FOUND() {
        return "Can’t find model named \"%s\". Please check and try again.";
    }

    public String getMODEL_MODIFY_ABANDON(String table) {
        return String.format(Locale.ROOT, "Model is not support to modify because you do not have permission of '%s'",
                table);
    }

    public String getEMPTY_PROJECT_NAME() {
        return "Can’t find project information. Please select a project.";
    }

    public String getGRANT_TABLE_WITH_SID_HAS_NOT_PROJECT_PERMISSION() {
        return "Failed to add table-level permissions.  User  (group)  [%s] has no project [%s] permissions.  Please grant user (group) project-level permissions first.";
    }

    public String getCheckCCType() {
        return "The actual data type \"{1}\" of computed column \"{0}\" is inconsistent with the defined type \"{2}\". Please modify it.";
    }

    public String getCheckCCExpression() {
        return "Can’t validate the expression \"%s\" (computed column: %s). Please check the expression, or try again later.";
    }

    public String getMODEL_METADATA_PACKAGE_INVALID() {
        return "Can’t parse the file. Please ensure that the file is complete.";
    }

    public String getEXPORT_BROKEN_MODEL() {
        return "Can’t export model \"%s\"  as it’s in \"BROKEN\" status. Please re-select and try again.";
    }

    public String getIMPORT_BROKEN_MODEL() {
        return "Model [%s] is broken, can not export.";
    }

    public String getIMPORT_MODEL_EXCEPTION() {
        return "Can’t import the model.";
    }

    public String getUN_SUITABLE_IMPORT_TYPE(String optionalType) {
        if (optionalType == null) {
            return "Can’t select ImportType \"%s\" for the model \"%s\". Please select \"UN_IMPORT\".";
        } else {
            return "Can’t select ImportType \"%s\" for the model \"%s\". Please select \"UN_IMPORT\" (or \""
                    + optionalType + "\").";
        }
    }

    public String getCAN_NOT_OVERWRITE_MODEL() {
        return "Can’t overwrite the model \"%s\", as it doesn’t exist. Please re-select and try again.";
    }

    public String getILLEGAL_MODEL_METADATA_FILE() {
        return "Can’t parse the metadata file. Please don’t modify the content or zip the file manually after unzip.";
    }

    public String getEXPORT_AT_LEAST_ONE_MODEL() {
        return "Please select at least one model to export.";
    }

    public String getCOMPUTED_COLUMN_EXPRESSION_DUPLICATED() {
        return "The expression of computed column has already been used in model '%s' as '%s'. Please modify the name to keep consistent, or use a different expression.";
    }

    public String getCOMPUTED_COLUMN_EXPRESSION_DUPLICATED_SINGLE_MODEL() {
        return "This expression has already been used by other computed columns in this model. Please modify it.";
    }

    public String getCOMPUTED_COLUMN_NAME_DUPLICATED() {
        return "The name of computed column '%s' has already been used in model '%s', and the expression is '%s'. Please modify the expression to keep consistent, or use a different name.";
    }

    public String getCOMPUTED_COLUMN_NAME_DUPLICATED_SINGLE_MODEL() {
        return "This name has already been used by other computed columns in this model. Please modify it.";
    }

    public String getMODEL_CHANGE_PERMISSION() {
        return "Don’t have permission. The model’s owner could only be changed by system or project admin.";
    }

    public String getMODEL_OWNER_CHANGE_INVALID_USER() {
        return "This user can’t be set as the model’s owner. Please select system admin, project admin or management user.";
    }

    // index
    public String getINDEX_STATUS_TYPE_ERROR() {
        return "The parameter \"status\" only supports “NO_BUILD”, “ONLINE”, “LOCKED”, “BUILDING”.";
    }

    public String getINDEX_SOURCE_TYPE_ERROR() {
        return "The parameter \"sources\" only supports “RECOMMENDED_AGG_INDEX”, “RECOMMENDED_TABLE_INDEX”, “CUSTOM_AGG_INDEX”, “CUSTOM_TABLE_INDEX”.";
    }

    public String getINDEX_SORT_BY_ERROR() {
        return "The parameter \"sort_by\" only supports “last_modified”, “usage”, “data_size”.";
    }

    // Job
    public String getILLEGAL_TIME_FILTER() {
        return "The selected time filter is invalid. Please select again.";
    }

    public String getILLEGAL_EXECUTABLE_STATE() {
        return "The job status is invalid. Please select again.";
    }

    public String getILLEGAL_JOB_STATE() {
        return "The job status is invalid. The value must be “PENDING“, “RUNNING“, “FINISHED“, “ERROR” or “DISCARDED“. Please check and try again.";
    }

    public String getILLEGAL_JOB_ACTION() {
        return "Invalid value in parameter “action“ or “statuses“ or “job_ids“,  " //
                + "The value of “statuses“ or the status of jobs specified by “job_ids“ contains “%s“, "
                + "this status of jobs can only perform the following actions: “%s“ .";
    }

    public String getILLEGAL_STATE_TRANSFER() {
        return "An error occurred when updating the job status. Please refresh the job list and try again.";
    }

    public String getINVALID_PRIORITY() {
        return "The selected priority is invalid. Please select a value within the range from 0 to 4.";
    }

    // Acl
    public String getUSER_NOT_EXIST() {
        return "User '%s' does not exist. Please make sure the user exists.";
    }

    public String getUSERGROUP_NOT_EXIST() {
        return "Invalid values in parameter “group_name“. The value %s doesn’t exist.";
    }

    public String getUSERGROUP_EXIST() {
        return "The user group \"%s\" already exists. Please check and try again.";
    }

    // user group
    public String getEMPTY_GROUP_NAME() {
        return "User group name should not be empty.";
    }

    public String getEMPTY_SID() {
        return "User/Group name should not be empty.";
    }

    public String getEMPTY_QUERY_NAME() {
        return "Query name should not be empty.";
    }

    public String getINVALID_QUERY_NAME() {
        return "Query name should only contain alphanumerics and underscores.";
    }

    //user
    public String getEMPTY_USER_NAME() {
        return "Username should not be empty.";
    }

    public String getSHORT_PASSWORD() {
        return "The password should contain more than 8 characters!";
    }

    public String getINVALID_PASSWORD() {
        return "The password should contain at least one number, letter and special character (~!@#$%^&*(){}|:\"<>?[];\\'\\,./`).";
    }

    public String getPERMISSION_DENIED() {
        return "Permission denied!";
    }

    public String getSELF_DELETE_FORBIDDEN() {
        return "Cannot delete yourself!";
    }

    public String getSELF_DISABLE_FORBIDDEN() {
        return "Cannot disable yourself!";
    }

    public String getSELF_EDIT_FORBIDDEN() {
        return "The object is invalid. Please check and try again.";
    }

    public String getUSER_EDIT_NOT_ALLOWED() {
        return "User editing operations under the LDAP authentication mechanism are not supported at this time";
    }

    public String getUSER_EDIT_NOT_ALLOWED_FOR_CUSTOM() {
        return "User editing is not allowed unless in current custom profile, function '%s' not implemented";
    }

    public String getGroup_EDIT_NOT_ALLOWED() {
        return "Group editing is not allowed unless in testing profile, please go to LDAP/SAML provider instead";
    }

    public String getGroup_EDIT_NOT_ALLOWED_FOR_CUSTOM() {
        return "Group editing is not allowed unless in current custom profile, function '%s' not implemented";
    }

    public String getOLD_PASSWORD_WRONG() {
        return "Old password is not correct!";
    }

    public String getNEW_PASSWORD_SAME_AS_OLD() {
        return "New password should not be same as old one!";
    }

    public String getUSER_AUTH_FAILED() {
        return "Invalid username or password. Please check and try again.";
    }

    public String getINVALID_EXECUTE_AS_USER() {
        return "User [%s] in the executeAs field does not exist";
    }

    public String getSERVICE_ACCOUNT_NOT_ALLOWED() {
        return "User [%s] does not have permissions for all tables, rows, and columns in the project [%s] and cannot use the executeAs parameter";
    }

    public String getEXECUTE_AS_NOT_ENABLED() {
        return "Configuration item \"kylin.query.query-with-execute-as\" is not enabled. So you cannot use the \"executeAs\" parameter now";
    }

    public String getUSER_BE_LOCKED(long seconds) {
        return "Invalid username or password. Please try again after " + formatSeconds(seconds) + ".";
    }

    public String getUSER_IN_LOCKED_STATUS(long leftSeconds, long nextLockSeconds) {
        return "For security concern, account %s has been locked. Please try again in " + formatSeconds(leftSeconds)
                + ". " + formatNextLockDuration(nextLockSeconds) + ".";
    }

    protected String formatNextLockDuration(long nextLockSeconds) {
        if (Long.MAX_VALUE == nextLockSeconds) {
            return "Login failure again will be locked permanently.";
        }
        return "Login failure again will be locked for " + formatSeconds(nextLockSeconds) + ".";
    }

    protected String formatSeconds(long seconds) {
        long remainingSeconds = seconds % 60;
        long remainingMinutes = ((seconds - remainingSeconds) / 60) % 60;
        long remainingHour = ((seconds - remainingSeconds - remainingMinutes * 60) / 3600) % 24;
        long remainingDay = (seconds - remainingSeconds - remainingMinutes * 60 - remainingHour * 3600) / (3600 * 24);
        String formatTimeMessage = formatTime(remainingDay, remainingHour, remainingMinutes, remainingSeconds);
        return formatTimeMessage.length() > 0 ? formatTimeMessage.substring(0, formatTimeMessage.length() - 1)
                : formatTimeMessage;
    }

    protected String formatTime(long day, long hour, long min, long second) {
        StringBuilder stringBuilder = new StringBuilder();
        if (day > 0) {
            stringBuilder.append(day).append(" days ");
        }
        if (hour > 0) {
            stringBuilder.append(hour).append(" hours ");
        }
        if (min > 0) {
            stringBuilder.append(min).append(" minutes ");
        }
        if (second > 0) {
            stringBuilder.append(second).append(" seconds ");
        }
        return stringBuilder.toString();
    }

    public String getUSER_IN_PERMANENTLY_LOCKED_STATUS() {
        return "User %s is locked permanently. Please contact to your administrator to reset.";
    }

    public String getOWNER_CHANGE_ERROR() {
        return "The change failed. Please try again.";
    }

    // Project
    public String getINVALID_PROJECT_NAME() {
        return "Please use number, letter, and underline to name your project, and start with a number or a letter.";
    }

    public String getPROJECT_NAME_IS_ILLEGAL() {
        return "The project name can’t exceed 50 characters. Please re-enter.";
    }

    public String getPROJECT_ALREADY_EXIST() {
        return "The project name \"%s\" already exists. Please rename it.";
    }

    public String getPROJECT_NOT_FOUND() {
        return "Can't find project \"%s\". Please check and try again.";
    }

    public String getPROJECT_DROP_FAILED_SECOND_STORAGE_ENABLED() {
        return "Can't delete project \"%s\", please disable tiered storage firstly.";
    }

    public String getPROJECT_DROP_FAILED_JOBS_NOT_KILLED() {
        return "Can't delete project \"%s\", please discard the related job and try again.";
    }

    public String getPROJECT_UNMODIFIABLE_REASON() {
        return "Model recommendation is not supported for this project at the moment. Please turn on the recommendation mode in project setting, and try again.";
    }

    public String getPROJECT_ONGOING_OPTIMIZATION() {
        return "System is optimizing historical queries at the moment. Please try again later. ";
    }

    public String getPROJECT_CHANGE_PERMISSION() {
        return "Don’t have permission. The owner of project could only be changed by system admin.";
    }

    public String getPROJECT_OWNER_CHANGE_INVALID_USER() {
        return "This user can’t be set as the project’s owner. Please select system admin, or the admin of this project.";
    }

    public String getPROJECT_DISABLE_MLP() {
        return "The multilevel partitioning is not enabled for this project. Please enable it in the project setting and try again.";
    }

    public String getTABLE_PARAM_EMPTY() {
        return "Can’t find the table. Please check and try again";
    }

    public String getTABLE_DESC_NOT_FOUND() {
        return "Cannot find table descriptor '%s'.";
    }

    // Table
    public String getTABLE_NOT_FOUND() {
        return "Can’t find table \"%s\". Please check and try again.";
    }

    public String getBEYOND_MIX_SAMPLING_ROWSHINT() {
        return "The number of sampling rows should be greater than %d. Please modify it.";
    }

    public String getBEYOND_MAX_SAMPLING_ROWS_HINT() {
        return "The number of sampling rows should be smaller than %d. Please modify it.";
    }

    public String getSAMPLING_FAILED_FOR_ILLEGAL_TABLE_NAME() {
        return "The name of table for sampling is invalid. Please enter a table name like “database.table”. ";
    }

    public String getFAILED_FOR_NO_SAMPLING_TABLE() {
        return "Can’t perform table sampling. Please select at least one table.";
    }

    public String getRELOAD_TABLE_CC_RETRY() {
        return "%1$s The data type of column %3$s in table %2$s has been changed. Please try deleting the computed column or changing the data type.";
    }

    public String getRELOAD_TABLE_MODEL_RETRY() {
        return "The data type of column %2$s from the source table %1$s has changed. Please remove the column from model %3$s, or modify the data type.";
    }

    public String getSAME_TABLE_NAME_EXIST() {
        return "Table %s already exists, please choose a different name.";
    }

    public String getQUERY_NOT_ALLOWED() {
        return "Job node is unavailable for queries. Please select a query node.";
    }

    public String getNOT_SUPPORTED_SQL() {
        return "This SQL is not supported at the moment. Please try a different SQL.";
    }

    public String getQUERY_TOO_MANY_RUNNING() {
        return "Can’t submit query at the moment as there are too many ongoing queries. Please try again later, or contact project admin to adjust configuration.";
    }

    public String getEXPORT_RESULT_NOT_ALLOWED() {
        return "Don’t have permission to export the query result. Please contact admin if needed.";
    }

    public String getFORBIDDEN_EXPORT_ASYNC_QUERY_RESULT() {
        return "Access denied. Only task submitters or admin users can download the query results";
    }

    public String getDUPLICATE_QUERY_NAME() {
        return "Query named \"%s\" already exists. Please check and try again.";
    }

    public String getNULL_EMPTY_SQL() {
        return "SQL can’t be empty. Please check and try again.";
    }

    public String getJOB_REPEATED_START_FAILURE() {
        return "Can’t start the streaming job repeatedly.";
    }

    public String getJOB_START_FAILURE() {
        return "Can’t start the streaming job of model \"%s\", as it has an ongoing one at the moment. Please check and try again.";
    }

    public String getJOB_BROKEN_MODEL_START_FAILURE() {
        return "Can’t start. Model \"%s\" is currently broken. ";
    }

    public String getJOB_STOP_FAILURE() {
        return "Can’t stop the streaming job of model \"%s\" at the moment. Please check the log or try again later.";
    }

    // Access
    public String getACL_PERMISSION_REQUIRED() {
        return "Acl permission required.";
    }

    public String getSID_REQUIRED() {
        return "Sid required.";
    }

    public String getREVOKE_ADMIN_PERMISSION() {
        return "Can't revoke admin permission of owner.";
    }

    public String getEMPTY_PERMISSION() {
        return "Permission should not be empty.";
    }

    public String getINVALID_PERMISSION() {
        return "Invalid values in parameter \"permission\". The value should either be \"ADMIN\", \"MANAGEMENT\", \"OPERATION\" or \"QUERY\".";
    }

    public String getINVALID_PARAMETER_TYPE() {
        return "Invalid value in parameter \"type\". The value should either be \"user\" or \"group\".";
    }

    public String getUNAUTHORIZED_SID() {
        return "The user/group doesn’t have access to this project.";
    }

    public String getACCESS_DENY() {
        return "The user doesn't have access.";
    }

    // Async Query
    public String getQUERY_RESULT_NOT_FOUND() {
        return "Can’t find the query by this query ID in this project. Please check and try again.";
    }

    public String getQUERY_RESULT_FILE_NOT_FOUND() {
        return "Can’t find the file of query results. Please check and try again.";
    }

    public String getQUERY_EXCEPTION_FILE_NOT_FOUND() {
        return "Can’t get the query status for the failed async query. Please check and try again.";
    }

    public String getCLEAN_FOLDER_FAIL() {
        return "Can’t clean file folder at the moment. Please ensure that the related file on HDFS could be accessed.";
    }

    public String getASYNC_QUERY_TIME_FORMAT_ERROR() {
        return "The time format is invalid. Please enter the date in the format “yyyy-MM-dd HH:mm:ss”.";
    }

    public String getASYNC_QUERY_PROJECT_NAME_EMPTY() {
        return "The project name can’t be empty. Please check and try again.";
    }

    // User
    public String getAUTH_INFO_NOT_FOUND() {
        return "Cannot find authentication information.";
    }

    public String getUSER_NOT_FOUND() {
        return "User '%s' not found.";
    }

    public String getDIAG_PACKAGE_NOT_AVAILABLE() {
        return "Diagnostic package is not available in directory: %s.";
    }

    public String getDIAG_FAILED() {
        return "Can’t generate diagnostic package. Please try regenerating it.";
    }

    // Basic
    public String getFREQUENCY_THRESHOLD_CAN_NOT_EMPTY() {
        return "The query frequency threshold cannot be empty";
    }

    public String getRECOMMENDATION_LIMIT_NOT_EMPTY() {
        return "The limit of recommendations for adding index cannot be empty.";
    }

    public String getDELAY_THRESHOLD_CAN_NOT_EMPTY() {
        return "The query delay threshold cannot be empty";
    }

    public String getMIN_HIT_COUNT_NOT_EMPTY() {
        return "The hits cannot be empty";
    }

    public String getEFFECTIVE_DAYS_NOT_EMPTY() {
        return "The time frame cannot be empty";
    }

    public String getUPDATE_FREQUENCY_NOT_EMPTY() {
        return "The recommendation frequency cannot be empty";
    }

    public String getSQL_NUMBER_EXCEEDS_LIMIT() {
        return "Up to %s SQLs could be imported at a time";
    }

    public String getSQL_LIST_IS_EMPTY() {
        return "Please enter the parameter “sqls“.";
    }

    public String getSQL_FILE_TYPE_MISMATCH() {
        return "The suffix of sql files must be 'txt' or 'sql'.";
    }

    public String getConfigNotSupportDelete() {
        return "Can’t delete this configuration. ";
    }

    public String getConfigNotSupportEdit() {
        return "Can’t edit this configuration. ";
    }

    public String getConfigMapEmpty() {
        return "The configuration list can’t be empty. Please check and try again.";
    }

    // Query statistics

    public String getNOT_SET_INFLUXDB() {
        return "Not set kap.metric.write-destination to 'INFLUX'";
    }

    // License
    public String getLICENSE_ERROR_PRE() {
        return "The license couldn’t be updated:\n";
    }

    public String getLICENSE_ERROR_SUFF() {
        return "\nPlease upload a new license, or contact Kyligence.";
    }

    public String getLICENSE_OVERDUE_TRIAL() {
        return "The license has expired and the validity period is [%s - %s]. Please upload a new license or contact Kyligence.";
    }

    public String getLICENSE_NODES_EXCEED() {
        return "The number of nodes which you are using is higher than the allowable number. Please contact your Kyligence account manager.";
    }

    public String getLICENSE_NODES_NOT_MATCH() {
        return "The cluster information dose not match the license. Please upload a new license or contact Kyligence.";
    }

    public String getLICENSE_OVER_VOLUME() {
        return "The current used system capacity exceeds the license’s limit. Please upload a new license, or contact Kyligence.";
    }

    public String getLICENSE_WRONG_CATEGORY() {
        return "The current version of Kyligence Enterprise does not match the license. Please upload a new license or contact Kyligence.";
    }

    public String getLICENSE_NO_LICENSE() {
        return "No license file. Please contact Kyligence.";
    }

    public String getLICENSE_INVALID_LICENSE() {
        return "The license is invalid. Please upload a new license, or contact Kyligence.";
    }

    public String getLICENSE_MISMATCH_LICENSE() {
        return "The license doesn’t match the current cluster information. Please upload a new license, or contact Kyligence.";
    }

    public String getLICENSE_NOT_EFFECTIVE() {
        return "License is not effective yet, please apply for a new license.";
    }

    public String getLICENSE_EXPIRED() {
        return "The license has expired. Please upload a new license, or contact Kyligence.";
    }

    public String getLICENSE_SOURCE_OVER_CAPACITY() {
        return "The amount of data volume used（%s/%s) exceeds the license’s limit. Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try deleting some segments.";
    }

    public String getLICENSE_PROJECT_SOURCE_OVER_CAPACITY() {
        return "The amount of data volume used（%s/%s) exceeds the project’s limit. Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try deleting some segments.";
    }

    public String getLICENSE_NODES_OVER_CAPACITY() {
        return "The amount of nodes used (%s/%s) exceeds the license’s limit. Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try stopping some nodes.";
    }

    public String getLICENSE_SOURCE_NODES_OVER_CAPACITY() {
        return "The amount of data volume used (%s/%s)  and nodes used (%s/%s) exceeds license’s limit.\n"
                + "Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try deleting some segments and stopping some nodes.";
    }

    public String getLICENSE_PROJECT_SOURCE_NODES_OVER_CAPACITY() {
        return "The amount of data volume used (%s/%s)  and nodes used (%s/%s) exceeds project’s limit.\n"
                + "Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try deleting some segments and stopping some nodes.";
    }

    public String getTABLENOTFOUND() {
        return "Can’t save model \"%s\". Please ensure that the used column \"%s\" exist in source table \"%s\".";
    }

    // Async push down get date format
    public String getPUSHDOWN_PARTITIONFORMAT_ERROR() {
        return "Can’t detect at the moment. Please set the partition format manually.";
    }

    // Async push down get data range
    public String getPUSHDOWN_DATARANGE_ERROR() {
        return "Can’t detect at the moment. Please set the data range manually.";
    }

    public String getPUSHDOWN_DATARANGE_TIMEOUT() {
        return "Can’t detect at the moment. Please set the data range manually.";
    }

    public String getDIMENSION_NOTFOUND() {
        return "The dimension %s is referenced by indexes or aggregate groups. Please go to the Data Asset - Model - Index page to view, delete referenced aggregate groups and indexes.";
    }

    public String getMEASURE_NOTFOUND() {
        return "The measure %s is referenced by indexes or aggregate groups. Please go to the Data Asset - Model - Index page to view, delete referenced aggregate groups and indexes.";
    }

    public String getNESTED_CC_CASCADE_ERROR() {
        return "Can’t modify computed column \"%s\". It’s been referenced by a nested computed column \"%s\" in the current model. Please remove it from the nested column first.";
    }

    public String getCC_ON_ANTI_FLATTEN_LOOKUP() {
        return "Can’t use the columns from dimension table “%s“ for computed columns, as the join relationships of this table won’t be precomputed.";
    }

    public String getFILTER_CONDITION_ON_ANTI_FLATTEN_LOOKUP() {
        return "Can’t use the columns from dimension table “%s“ for data filter condition, as the join relationships of this table won’t be precomputed.";
    }

    public String getCHANGE_GLOBALADMIN() {
        return "You cannot add,modify or remove the system administrator’s rights";
    }

    public String getCHANGE_DEGAULTADMIN() {
        return "Can’t modify the permission of user “ADMIN”, as this user is a built-in admin by default.";
    }

    //Query
    public String getINVALID_USER_TAG() {
        return "Can’t add the tag, as the length exceeds the maximum 256 characters. Please modify it.";
    }

    public String getINVALID_ID() {
        return "Can’t find ID \"%s\". Please check and try again.";
    }

    public String getSEGMENT_LOCKED() {
        return "Can’t remove, refresh or merge segment \"%s\", as it’s LOCKED. Please try again later.";
    }

    public String getSEGMENT_STATUS(String status) {
        return "Can’t refresh or merge segment \"%s\", as it’s in \"" + status + "\".Please try again later.";
    }

    //HA
    public String getNO_ACTIVE_LEADERS() {
        return "There is no active leader node. Please contact system admin to check and fix.";
    }

    //Kerberos
    public String getPRINCIPAL_EMPTY() {
        return "Principal name cannot be null.";
    }

    public String getKEYTAB_FILE_TYPE_MISMATCH() {
        return "The suffix of keytab file must be 'keytab'.";
    }

    public String getKERBEROS_INFO_ERROR() {
        return "Invalid principal name or keytab file, please check and submit again.";
    }

    public String getPROJECT_HIVE_PERMISSION_ERROR() {
        return "Permission denied. Please confirm the Kerberos account can access all the loaded tables.";
    }

    public String getLEADERS_HANDLE_OVER() {
        return "System is trying to recover service. Please try again later.";
    }

    public String getTABLE_REFRESH_ERROR() {
        return "Can’t connect to data source due to unexpected error. Please refresh and try again.";
    }

    public String getTABLE_REFRESH_NOTFOUND() {
        return "Can’t connect to data source due to unexpected error. Please refresh and try again.";
    }

    public String getTABLE_REFRESH_PARAM_INVALID() {
        return "The “table“ parameter is invalid. Please check and try again.";
    }

    public String getTABLE_REFRESH_PARAM_MORE() {
        return "There exists invalid filed(s) other than the expected “tables“. Please check and try again.";
    }

    public String getTRANSFER_FAILED() {
        return "Can’t transfer the request at the moment. Please try again later.";
    }

    public String getUSER_EXISTS() {
        return "Username:[%s] already exists";
    }

    public String getOPERATION_FAILED_BY_USER_NOT_EXIST() {
        return "Operation failed, user:[%s] not exists, please add it first";
    }

    public String getOPERATION_FAILED_BY_GROUP_NOT_EXIST() {
        return "Operation failed, group:[%s] not exists, please add it first";
    }

    public String getCOLUMU_IS_NOT_DIMENSION() {
        return "Please add column \"%s\" as a dimension.";
    }

    public String getMODEL_CAN_NOT_PURGE() {
        return "Can’t purge data by specifying model \"%s\" under the current project settings.";
    }

    public String getMODEL_SEGMENT_CAN_NOT_REMOVE() {
        return "Can’t delete the segment(s) in model \"%s\" under the current project settings.";
    }

    public String getSEGMENT_CAN_NOT_REFRESH() {
        return "Can’t refresh some segments, as they are being built at the moment. Please try again later.";
    }

    public String getSEGMENT_CAN_NOT_REFRESH_BY_SEGMENT_CHANGE() {
        return "Can’t refresh at the moment, as the segment range has changed. Please try again later.";
    }

    public String getCAN_NOT_BUILD_SEGMENT() {
        return "Can’t build segment(s). Please add some indexes first.";
    }

    public String getCAN_NOT_BUILD_SEGMENT_MANUALLY() {
        return "Can’t manually build segments in model \"%s\" under the current project settings.";
    }

    public String getCAN_NOT_BUILD_INDICES_MANUALLY() {
        return "Can’t manually build indexes in model \"%s\" under the current project settings.";
    }

    public String getINVALID_MERGE_SEGMENT() {
        return "Can’t merge segments which are not ready yet.";
    }

    public String getINVALID_SET_TABLE_INC_LOADING() {
        return "Can‘t set table \"%s\" as incremental loading. It’s been used as a dimension table in model \"%s\".";
    }

    public String getINVALID_REFRESH_SEGMENT_BY_NO_SEGMENT() {
        return "There is no ready segment to refresh at the moment. Please try again later.";
    }

    public String getINVALID_REFRESH_SEGMENT_BY_NOT_READY() {
        return "Can’t refresh at the moment. Please ensure that all segments within the refresh range is ready.";
    }

    public String getINVALID_LOAD_HIVE_TABLE_NAME() {
        return "Can’t operate now. Please set “kap.table.load-hive-tablename-cached.enabled=true”, and try again.";
    }

    public String getINVALID_REMOVE_USER_FROM_ALL_USER() {
        return "Can not remove user from ALL USERS group.";
    }

    public String getACCESS_DENY_ONLY_ADMIN() {
        return "Access Denied, only system and project administrators can edit users' tables, columns, and rows permissions";
    }

    public String getACCESS_DENY_ONLY_ADMIN_AND_PROJECT_ADMIN() {
        return "Access Denied, only system administrators can edit users' tables, columns, and rows permissions";
    }

    public String getSEGMENT_LIST_IS_EMPTY() {
        return "Can’t find the segment(s). Please check and try again.";
    }

    public String getSEGMENT_ID_NOT_EXIST() {
        return "Can’t find the segment by ID \"%s\". Please check and try again.";
    }

    public String getSEGMENT_NAME_NOT_EXIST() {
        return "Can’t find the segment by name \"%s\". Please check and try again.";
    }

    public String getLAYOUT_LIST_IS_EMPTY() {
        return "Can’t find the layout(s). Please check and try again.";
    }

    public String getLAYOUT_NOT_EXISTS() {
        return "Can’t find layout \"%s\". Please check and try again.";
    }

    public String getINVALID_REFRESH_SEGMENT() {
        return "Please select at least one segment to refresh.";
    }

    public String getEMPTY_SEGMENT_PARAMETER() {
        return "Please enter segment ID or name.";
    }

    public String getCONFLICT_SEGMENT_PARAMETER() {
        return "Can’t enter segment ID and name at the same time. Please re-enter.";
    }

    public String getINVALID_MERGE_SEGMENT_BY_TOO_LESS() {
        return "Please select at least two segments to merge.";
    }

    public String getCONTENT_IS_EMPTY() {
        return "license content is empty";
    }

    public String getILLEGAL_EMAIL() {
        return "A personal email or illegal email is not allowed";
    }

    public String getLICENSE_ERROR() {
        return "Get license error";
    }

    public String getEMAIL_USERNAME_COMPANY_CAN_NOT_EMPTY() {
        return "Email, username, company can not be empty";
    }

    public String getEMAIL_USERNAME_COMPANY_IS_ILLEGAL() {
        return "Email, username, company length should be less or equal than 50";
    }

    public String getUSERNAME_COMPANY_IS_ILLEGAL() {
        return "Username, company only supports Chinese, English, numbers, spaces";
    }

    public String getINVALID_COMPUTER_COLUMN_NAME_WITH_KEYWORD() {
        return "The computed column name \"%s\" is a SQL keyword. Please choose another name.";
    }

    public String getINVALID_COMPUTER_COLUMN_NAME() {
        return "The computed column name \"%s\" is invalid. Please starts with a letter, and use only letters, numbers, and underlines. Please rename it.";
    }

    public String getMODEL_ALIAS_DUPLICATED() {
        return "Model \"%s\" already exists. Please rename it.";
    }

    public String getINVALID_RANGE_LESS_THAN_ZERO() {
        return "The start and end time should be greater than 0. Please modify it.";
    }

    public String getINVALID_RANGE_NOT_FORMAT() {
        return "The format of start or end time is invalid. Only supports timestamp with time unit of millisecond (ms). Please modify it.";
    }

    public String getINVALID_RANGE_END_LESSTHAN_START() {
        return "The end time must be greater than the start time. Please modify it.";
    }

    public String getINVALID_RANGE_NOT_CONSISTENT() {
        return "The start and end time must exist or not exist at the same time. Please modify it.";
    }

    public String getID_CANNOT_EMPTY() {
        return "ID can’t be empty. Please check and try again.";
    }

    public String getINVALID_CREATE_MODEL() {
        return "Can’t add model manually under this project.";
    }

    public String getSEGMENT_INVALID_RANGE() {
        return "Can’t refresh. The segment range \"%s\" exceeds the range of loaded data, which is \"%s\". Please modify and try again.";
    }

    public String getSEGMENT_RANGE_OVERLAP() {
        return "Can’t build segment. The specified data range overlaps with the built segments from \"%s\" to \"%s\". Please modify and try again.";
    }

    public String getPARTITION_COLUMN_NOT_EXIST() {
        return "Can’t find the partition column. Please check and try again.";
    }

    public String getPARTITION_COLUMN_START_ERROR() {
        return "Can’t start. Please ensure the time partition column is a timestamp column and the time format is valid.";
    }

    public String getPARTITION_COLUMN_SAVE_ERROR() {
        return "Can’t submit. Please ensure the time partition column is a timestamp column and the time format is valid.";
    }

    public String getTIMESTAMP_COLUMN_NOT_EXIST() {
        return "Can’t load. Please ensure the table has at least a timestamp column.";
    }

    public String getTIMESTAMP_PARTITION_COLUMN_NOT_EXIST() {
        return "Can’t save the model. For fusion model, the time partition column must be added as a dimension.";
    }

    public String getINVALID_PARTITION_COLUMN() {
        return "Please select an original column (not a computed column) from the fact table as time partition column.";
    }

    public String getTABLE_NAME_CANNOT_EMPTY() {
        return "Table name can’t be empty. Please check and try again.";
    }

    public String getTABLE_SAMPLE_MAX_ROWS() {
        return "The sampling range should between 10,000 and 20,000,000 rows.";
    }

    public String getFILE_NOT_EXIST() {
        return "Cannot find file [%s]";
    }

    public String getDATABASE_NOT_EXIST() {
        return "Can’t find database \"%s\". Please check and try again.";
    }

    public String getBROKEN_MODEL_CANNOT_ONOFFLINE() {
        return "Can’t get model \"%s\" online or offline, as it’s in BROKEN state.";
    }

    public String getINVALID_NAME_START_WITH_DOT() {
        return "The user / group names cannot start with a period (.).";
    }

    public String getINVALID_NAME_START_OR_END_WITH_BLANK() {
        return "User / group names cannot start or end with a space.";
    }

    public String getINVALID_NAME_LEGTHN() {
        return "The username should contain less than 180 characters. Please check and try again.";
    }

    public String getINVALID_NAME_CONTAINS_OTHER_CHARACTER() {
        return "Only alphanumeric characters can be used in user / group names";
    }

    public String getINVALID_NAME_CONTAINS_INLEGAL_CHARACTER() {
        return "The user / group names cannot contain the following symbols: backslash (\\), "
                + " slash mark (/), colon (:), asterisk (*), question mark (?),  quotation mark (\"), less than sign (<), greater than sign (>), vertical bar (|).";

    }

    public String getHIVETABLE_NOT_FOUND() {
        return "Can’t load table \"%s\". Please ensure that the table(s) could be found in the data source.";
    }

    public String getDUPLICATE_LAYOUT() {
        return "Can’t add this index, as the same index already exists. Please modify.";
    }

    public String getDEFAULT_REASON() {
        return "Something went wrong. %s";
    }

    public String getDEFAULT_SUGGEST() {
        return "Please contact Kyligence technical support for more details.";
    }

    public String getUNEXPECTED_TOKEN() {
        return "Syntax error occurred at line %s, column %s: \"%s\". Please modify it.";
    }

    public String getBAD_SQL_REASON() {
        return "The SQL has syntax error: %s ";
    }

    public String getBAD_SQL_SUGGEST() {
        return "Please modify it.";
    }

    public String getBAD_SQL_TABLE_NOT_FOUND_REASON() {
        return "Can’t find table \"%s\". Please check and try again.";
    }

    public String getBAD_SQL_TABLE_NOT_FOUND_SUGGEST() {
        return "Please add table \"%s\" to data source. If this table does exist, mention it as \"DATABASE.TABLE\".";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_REASON() {
        return "Can’t find column \"%s\". Please check if it exists in the source table. If exists, please try reloading the table; if not exist, please contact admin to add it.";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_SUGGEST() {
        return "Can’t find column \"%s\". Please check if it exists in the source table. If exists, please try reloading the table; if not exist, please contact admin to add it.";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_REASON() {
        return "Can’t find column \"%s\". Please check if it exists in the source table. If exists, please try reloading the table; if not exist, please contact admin to add it.";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_SUGGESTION() {
        return "Can’t find column \"%s\". Please check if it exists in the source table. If exists, please try reloading the table; if not exist, please contact admin to add it.";
    }

    public String getPROJECT_NUM_OVER_THRESHOLD() {
        return "Failed to create a new project. The number of projects exceeds the maximum: {%s}. Please delete other abandoned projects before trying to create new ones or contact the administrator to adjust the maximum number of projects.";
    }

    public String getMODEL_NUM_OVER_THRESHOLD() {
        return "Failed to create a new model. The number of models exceeds the maximum: {%s}. Please delete other abandoned models before trying to create new ones or contact the administrator to adjust the maximum number of models.";
    }

    public String getQUERY_ROW_NUM_OVER_THRESHOLD() {
        return "Can’t get query result. The rows of query result exceeds the maximum limit \"%s\". Please add filters, or contact admin to adjust the maximum limit.";
    }

    public String getCC_EXPRESSION_CONFLICT(String newCCExpression, String newCCName, String existedCCName) {
        return String.format(Locale.ROOT,
                "The expression \"%s\" of computed column \"%s\" is same as computed column \"%s\". Please modify it.",
                newCCExpression, newCCName, existedCCName);
    }

    public String getCC_NAME_CONFLICT(String ccName) {
        return String.format(Locale.ROOT, "Computed column \"%s\" already exists. Please modify it.", ccName);
    }

    public String get_ALIAS_CONFLICT_OF_APPROVING_RECOMMENDATION() {
        return "The name already exists. Please rename and try again.";
    }

    public String getDIMENSION_CONFLICT(String dimensionName) {
        return String.format(Locale.ROOT, "Dimension \"%s\" already exists. Please modify it.", dimensionName);
    }

    public String getMEASURE_CONFLICT(String measureName) {
        return String.format(Locale.ROOT, "Measure \"%s\" already exists. Please modify it.", measureName);
    }

    public String getJOB_NODE_INVALID(String url) {
        return "Can’t execute this request on job node. Please check and try again.";
    }

    public String getINVALID_TIME_FORMAT() {
        return "Can’t set the time partition column. The values of the selected column is not time formatted. Please select again.";
    }

    public String getINVALID_CUSTOMIZE_FORMAT() {
        return "Unsupported format. Please check and re-enter.";
    }

    public String getSEGMENT_CONTAINS_GAPS() {
        return "Can’t merge the selected segments, as there are gap(s) in between. Please check and try again.";
    }

    public String getSegmentMergeLayoutConflictError() {
        return "The indexes included in the selected segments are not fully identical. Please build index first and try merging again.";
    }

    public String getSegmentMergePartitionConflictError() {
        return "The subpartitions included in the selected segments are not fully aligned. Please build the subpartitions first and try merging again.";
    }

    public String getSegmentMergeStorageCheckError() {
        return "During segment merging, the HDFS storage space may exceed the threshold limit, and the system actively terminates the merging job. If you need to remove the above restrictions, please refer to the user manual to adjust the parameter kylin.cube.merge-segment-storage-threshold.";
    }

    public String getDIMENSION_TABLE_USED_IN_THIS_MODEL() {
        return "Can’t set the dimension table of this model, as it has been used as fact table in this model. Please modify and try again.";
    }

    public String getNO_DATA_IN_TABLE() {
        return "Can’t get data from table \"%s\". Please check and try again.";
    }

    public String getEFFECTIVE_DIMENSION_NOT_FIND() {
        return "The following columns are not added as dimensions to the model. Please delete them before saving or add them to the model.\nColumn ID: %s";
    }

    public String getINVALID_PASSWORD_ENCODER() {
        return "Illegal PASSWORD ENCODER, please check configuration item kylin.security.user-password-encoder";
    }

    public String getFAILED_INIT_PASSWORD_ENCODER() {
        return "PASSWORD ENCODER init failed, please check configuration item kylin.security.user-password-encoder";
    }

    public String getINVALID_NULL_VALUE() {
        return "Failed to rewrite the model settings, %s parameter value is null.";
    }

    public String getINVALID_INTEGER_FORMAT() {
        return "Can’t rewrite the model settings. The value of  \"%s\" must be non-negative integer. Please modify and try again.";
    }

    public String getINVALID_MEMORY_SIZE() {
        return "Can’t rewrite the model settings. The value of \"spark-conf.spark.executor.memory\" must be non-negative integer, with the unit of GB. Please modify and try again.";
    }

    public String getINVALID_BOOLEAN_FORMAT() {
        return "Can’t rewrite the model settings. The value of \"%s\" must be either “true” or “false”. Please modify and try again.";
    }

    public String getINVALID_AUTO_MERGE_CONFIG() {
        return "Can’t rewrite the model settings. The automatic merge range can’t be empty. Please modify and try again.";
    }

    public String getINVALID_VOLATILE_RANGE_CONFIG() {
        return "Can’t rewrite the model settings. The unit of the dynamic interval parameter must be either \"day\", \"week\", \"month\", or \"year\", and the value must be non-negative integer. Please modify and try again.";
    }

    public String getINVALID_RETENTION_RANGE_CONFIG() {
        return "Failed to rewrite the model settings, parameter value must be non-negative integer and the "
                + "unit of parameter must be the coarsest granularity unit in the unit selected for automatic merge.";
    }

    public String getINSUFFICIENT_AUTHENTICATION() {
        return "Unable to authenticate. Please login again.";
    }

    public String getDISABLED_USER() {
        return "This user is disabled. Please contact admin.";
    }

    public String getWRITE_IN_MAINTENANCE_MODE() {
        return "System is currently undergoing maintenance. Metadata related operations are temporarily unavailable.";
    }

    public String getINVALID_SID_TYPE() {
        return "Invalid value for parameter ‘type’ which should only be ‘user’ or ‘group’(case-insensitive).";
    }

    public String getEMPTY_DATABASE_NAME() {
        return "Invalid value for parameter ‘database_name’ which should not be empty.";
    }

    public String getEMPTY_DATABASE() {
        return "Please enter the value for the parameter \"Database\". ";
    }

    public String getEMPTY_TABLE_LIST() {
        return "Please enter the value for the parameter \"Table\". ";
    }

    public String getEMPTY_TABLE_NAME() {
        return "Invalid value for parameter ‘table_name’ which should not be empty.";
    }

    public String getEMPTY_COLUMN_LIST() {
        return "Invalid value for parameter ‘columns’ which should not be empty.";
    }

    public String getEMPTY_ROW_LIST() {
        return "Invalid value for parameter ‘rows’ which should not be empty.";
    }

    public String getEMPTY_COLUMN_NAME() {
        return "Invalid value for parameter ‘column_name’ which should not be empty.";
    }

    public String getEMPTY_ITEMS() {
        return "Invalid value for parameter ‘items’ which should not be empty.";
    }

    public String getCOLUMN_NOT_EXIST() {
        return "Column:[%s] is not exist.";
    }

    public String getDATABASE_PARAMETER_MISSING() {
        return "All the databases should be defined and the database below are missing: (%s).";
    }

    public String getTABLE_PARAMETER_MISSING() {
        return "All the tables should be defined and the table below are missing: (%s).";
    }

    public String getCOLUMN_PARAMETER_MISSING() {
        return "All the columns should be defined and the column below are missing: (%s).";
    }

    public String getCOLUMN_PARAMETER_INVALID(String column) {
        return String.format(Locale.ROOT,
                "Can’t assign value(s) for the column \"%s\", as the value(s) doesn’t match the column’s data type. Please check and try again.",
                column);
    }

    public String getDATABASE_PARAMETER_DUPLICATE() {
        return "Database [%s] is duplicated in API requests.";
    }

    public String getTABLE_PARAMETER_DUPLICATE() {
        return "Table [%s] is duplicated in API requests.";
    }

    public String getCOLUMN_PARAMETER_DUPLICATE() {
        return "Column [%s] is duplicated in API requests.";
    }

    public String getADD_JOB_CHECK_FAIL() {
        return "Can’t submit the job at the moment, as a building job for the same object already exists. Please try again later.";
    }

    public String getADD_EXPORT_JOB_FAIL() {
        return "Can’t submit the job at the moment. There is an ongoing load data job of tiered storage for the same segment(s). Please try again later.";
    }

    public String getADD_JOB_CHECK_FAIL_WITHOUT_BASE_INDEX() {
        return "Can’t submit the job at the moment. The segment “%s” doesn’t have base index. Please refresh this segment.";
    }

    public String getADD_JOB_EXCEPTION() {
        return "Can’t find executable jobs at the moment. Please try again later.";
    }

    public String getADD_JOB_ABANDON() {
        return "Can’t submit the job to this node, as it’s not a job node. Please check and try again.";
    }

    public String getSTORAGE_QUOTA_LIMIT() {
        return "No storage quota available. The system failed to submit building job, while the query engine will still be available. "
                + "Please clean up low-efficient storage in time, increase the low-efficient storage threshold, or notify the administrator to increase the storage quota for this project.";
    }

    public String getADD_JOB_CHECK_SEGMENT_FAIL() {
        return "Can’t submit the job, as the indexes are not identical in the selected segments. Please check and try again.";
    }

    public String getADD_JOB_CHECK_SEGMENT_READY_FAIL() {
        return "Can’t submit the job, as there are no segments in READY status. Please try again later.";
    }

    public String getADD_JOB_CHECK_INDEX_FAIL() {
        return "Can’t submit the job, as no index is included in the segment. Please check and try again.";
    }

    public String getREFRESH_JOB_CHECK_INDEX_FAIL() {
        return "No index is available to be refreshed. Please check and try again.";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_ABANDON() {
        return "Can’t add the job. Please ensure that the operation is valid for the current object.";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_EMPTY() {
        return "Can’t add the job, as the subpartition value is empty. Please check and try again.";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_DUPLICATE() {
        return "Can’t add the job. Please ensure that the subpartitions are unique.";
    }

    public String getTABLE_RELOAD_ADD_COLUMN_EXIST(String table, String column) {
        return String.format(Locale.ROOT,
                "Can’t reload the table at the moment. Column \"%s\" already exists in table \"%s\". Please modify and try again.",
                table, column);
    }

    public String getTABLE_RELOAD_HAVING_NOT_FINAL_JOB() {
        return "The table metadata can’t be reloaded now. There are ongoing jobs with the following target subjects(s): %s. Please try reloading until all the jobs are completed, or manually discard the jobs.";
    }

    public String getCOLUMN_UNRECOGNIZED() {
        return "Can’t recognize column \"%s\". Please use \"TABLE_ALIAS.COLUMN\" to reference a column.";
    }

    public String getInvalidJobStatusTransaction() {
        return "Can’t %s job \"%s\", as it is in %s status.";
    }

    // Punctuations
    public String getCOMMA() {
        return ", ";
    }

    public String getREC_LIST_OUT_OF_DATE() {
        return "The recommendation is invalid, as some of the related content was deleted. Please refresh the page and try again.";
    }

    public String getGROUP_UUID_NOT_EXIST() {
        return "Can’t operate user group (UUID: %s). Please check and try again.";
    }

    public String getMODEL_ONLINE_WITH_EMPTY_SEG() {
        return "This model can’t go online as it doesn’t have segments. Models with no segment couldn’t serve queries. Please add a segment.";
    }

    public String getSCD2_MODEL_ONLINE_WITH_SCD2_CONFIG_OFF() {
        return "This model can’t go online as it includes non-equal join conditions(≥, <)."
                + " Please delete those join conditions,"
                + " or turn on 'Show non-equal join conditions for History table' in project settings.";
    }

    public String getCONNECT_DATABASE_ERROR() {
        return "Can’t connect to the RDBMS metastore. Please check if the metastore is working properly.";
    }

    // acl
    public String getInvalidColumnAccess() {
        return "The current user or user group doesn’t have access to the column \"%s\".";
    }

    public String getInvalidSensitiveDataMaskColumnType() {
        return "Can’t do data masking for the data with type of boolean, map or array.";
    }

    public String getNotSupportNestedDependentCol() {
        return "Can’t set association rules on the column \"%s\". This column has been associated with another column.";
    }

    public String getInvalidRowACLUpdate() {
        return "The parameter “rows” or “like_rows” is invalid. Please use the parameter “row_filter” to update the row ACL.";
    }

    // Snapshots
    public String getSNAPSHOT_OPERATION_PERMISSION_DENIED() {
        return "Don’t have permission. Please ensure that you have required permission to the table which this snapshot is associated with.";
    }

    public String getSNAPSHOT_NOT_FOUND() {
        return "Can't find the snapshot \"%s\". Please check and try again.";
    }

    public String getSNAPSHOT_MANAGEMENT_NOT_ENABLED() {
        return "Snapshot management is not enabled in the settings. Please check and try again.";
    }

    public String getINVALID_DIAG_TIME_PARAMETER() {
        return "The end time must be greater than the start time. Please modify it.";
    }

    public String getPARTITIONS_TO_BUILD_CANNOT_BE_EMPTY(List<String> tableDescNames) {
        return "Please select at least one partition for the following snapshots when conducting custom partition value refresh: "
                + tableDescNames.toString();
    }

    // Resource Group
    public String getRESOURCE_GROUP_FIELD_IS_NULL() {
        return "Can’t execute this request. Please ensure that all the parameters for the resource group request are included.";
    }

    public String getRESOURCE_CAN_NOT_BE_EMPTY() {
        return "Please ensure that at least one resource group exists once the resource group mode is enabled.";
    }

    public String getEMPTY_RESOURCE_GROUP_ID() {
        return "Resource group ID can’t be empty. Please check and try again.";
    }

    public String getDUPLICATED_RESOURCE_GROUP_ID(String entityId) {
        return String.format(Locale.ROOT, "The resource group ID \"%s\" already exists. Please check and try again.",
                entityId);
    }

    public String getRESOURCE_GROUP_DISABLED_WITH_INVLIAD_PARAM() {
        return "To disable the resource group mode, please remove all the instances and projects for the existing resource groups.";
    }

    public String getPROJECT_WITHOUT_RESOURCE_GROUP() {
        return "Can’t use this project properly as no resource group has been allocated yet. Please contact admin.";
    }

    public String getEMPTY_KYLIN_INSTANCE_IDENTITY() {
        return "Please enter a value for the parameter \"instance\".";
    }

    public String getEMPTY_KYLIN_INSTANCE_RESOURCE_GROUP_ID() {
        return "Please enter a value for the parameter \"resource_group_id\".";
    }

    public String getRESOURCE_GROUP_ID_NOT_EXIST_IN_KYLIN_INSTANCE(String id) {
        return String.format(Locale.ROOT,
                "Can’t find the parameter \"resource_group_id\" in the instance, which value is \"%s\". Please check and try again.",
                id);
    }

    public String getDUPLICATED_KYLIN_INSTANCE() {
        return "The same instance already exists. Please check and try again.";
    }

    public String getEMPTY_PROJECT_IN_MAPPING_INFO() {
        return "The project can’t be empty in the mapping_info. Please check and try again.";
    }

    public String getEMPTY_RESOURCE_GROUP_ID_IN_MAPPING_INFO() {
        return "The parameter \"resource_group_id\" can’t be empty in the mapping_info. Please check and try again.";
    }

    public String getPROJECT_BINDING_RESOURCE_GROUP_INVALID() {
        return "Can’t allocate resource group for project \"%s\". Please ensure that the project is allocated to two resource groups at most. Meanwhile, each request (query or build) is allocated to one resource group.";
    }

    public String getRESOURCE_GROUP_ID_NOT_EXIST_IN_MAPPING_INFO(String id) {
        return String.format(Locale.ROOT,
                "Can’t find the parameter \"resource_group_id\" (\"%s\") in the mapping_info. Please check and try again.",
                id);
    }

    public String getMODEL_ONLINE_FORBIDDEN() {
        return "Can’t get the model online. Please set the configuration “kylin.model.offline“ as false first.";
    }

    // multi level partition mapping
    public String getMULTI_PARTITION_MAPPING_REQEUST_NOT_VALID() {
        return "Can’t update the mapping relationships of the partition column. The value for the parameter “multi_partition_columns“ doesn’t match the partition column defined in the model. Please check and try again.";
    }

    public String getCONCURRENT_SUBMIT_JOB_LIMIT() {
        return "Can't submit building jobs, as it exceeds the concurrency limit (%s).  Please try submitting fewer jobs at a time.";
    }

    public String getMODEL_IS_NOT_MLP() {
        return "\"%s\" is not a multilevel partitioning model. Please check and try again.";
    }

    public String getINVALID_PARTITION_VALUE() {
        return "The subpartition(s) “%s“ doesn’t exist. Please check and try again.";
    }

    public String getPARTITION_VALUE_NOT_SUPPORT() {
        return "Model \"%s\" hasn’t set a partition column yet. Please set it first and try again.";
    }

    public String getADMIN_PERMISSION_UPDATE_ABANDON() {
        return "Admin is not supported to update permission.";
    }

    public String getMODEL_ID_NOT_EXIST() {
        return "The model with ID “%s” is not found.";
    }

    public String getNot_IN_EFFECTIVE_COLLECTION() {
        return "“%s“ is not a valid value. This parameter only supports “ONLINE”, “OFFLINE”, “WARNING”, “BROKEN“.";
    }

    public String getROW_ACL_NOT_STRING_TYPE() {
        return "The LIKE operator could only be used for the char or varchar data type. Please check and try again.";
    }

    public String getRowFilterExceedLimit() {
        return "The number of filters exceeds the upper limit (%s/%s). Please check and try again.";
    }

    public String getRowFilterItemExceedLimit() {
        return "The number of the included values of a single filter exceeds the upper limit (%s/%s). Please check and try again.";
    }

    public String getSTOP_BY_USER_ERROR_MESSAGE() {
        return "Stopped by user.";
    }

    public String getExceedMaxAllowedPacket() {
        return "The result packet of MySQL exceeds the limit. Please contact the admin to adjust the value of “max_allowed_packet“ as 256M in MySQL. ";
    }

    public String getQUERY_HISTORY_COLUMN_META() {
        return "Start Time,Duration,Query ID,SQL Statement,Answered by,Query Status,Query Node,Submitter,Query Message\n";
    }

    public String getSECOND_STORAGE_JOB_EXISTS() {
        return "Can’t turn off the tiered storage at the moment. Model “%s” has an ongoing job, Please try again later.\\n";
    }

    public String getSECOND_STORAGE_CONCURRENT_OPERATE() {
        return "Another tiered storage task is running. Please try again later.";
    }

    public String getSECOND_STORAGE_PROJECT_JOB_EXISTS() {
        return "Can’t turn off the tiered storage at the moment. Project “%s” has an ongoing job, Please try again later.\\n";
    }

    public String getSECOND_STORAGE_PROJECT_ENABLED() {
        return "Loading failed, the project %s does not have tiered storage enabled.";
    }

    public String getSECOND_STORAGE_MODEL_ENABLED() {
        return "Loading failed, the model %s does not have tiered storage enabled.";
    }

    public String getSECOND_STORAGE_SEGMENT_WITHOUT_BASE_INDEX() {
        return "The base table index is missing in the segments, please add and try again.";
    }

    public String getJOB_RESTART_FAILED() {
        return "Tiered storage task doesn't support restart.\n";
    }

    public String getSEGMENT_DROP_FAILED() {
        return "Segment can't remove. There is an ongoing load data job of tiered storage. Please try again later.\n";
    }

    public String getJOB_RESUME_FAILED() {
        return "Tiered storage task can't resume. Please try again later.\n";
    }

    public String getINVALID_BROKER_DEFINITION() {
        return "The broker filed can’t be empty. Please check and try again.";
    }

    public String getBROKER_TIMEOUT_MESSAGE() {
        return "Can’t get the cluster information. Please check whether the broker information is correct, or confirm whether the Kafka server status is normal.";
    }

    public String getSTREAMING_TIMEOUT_MESSAGE() {
        return "Can’t get sample data. Please check and try again.";
    }

    public String getEMPTY_STREAMING_MESSAGE() {
        return "This topic has no sample data. Please select another one.";
    }

    public String getINVALID_STREAMING_MESSAGE_TYPE() {
        return "The format is invalid. Only support json or binary at the moment. Please check and try again.";
    }

    public String getPARSE_STREAMING_MESSAGE_ERROR() {
        return "The parser cannot parse the sample data. Please check the options or modify the parser, and parse again.";
    }

    public String getREAD_KAFKA_JAAS_FILE_ERROR() {
        return "Can't read Kafka authentication file correctly. Please check and try again.";
    }

    public String getBATCH_STREAM_TABLE_NOT_MATCH() {
        return "The columns from table “%s“ and the Kafka table are not identical. Please check and try again.";
    }

    public String getSTREAMING_INDEXES_DELETE() {
        return "Can’t delete the streaming indexes. Please stop the streaming job and then delete all the streaming segments.";
    }

    public String getSTREAMING_INDEXES_EDIT() {
        return "Can’t edit the streaming indexes. Please stop the streaming job and then delete all the streaming segments.";
    }

    public String getSTREAMING_INDEXES_ADD() {
        return "Can’t add the streaming indexes. Please stop the streaming job and then delete all the streaming segments.";
    }

    public String getSTREAMING_INDEXES_APPROVE() {
        return "Streaming model can’t accept recommendations at the moment.";
    }

    public String getSTREAMING_INDEXES_CONVERT() {
        return "Streaming model can’t convert to recommendations at the moment.";
    }

    public String getCANNOT_FORCE_TO_BOTH_PUSHDODWN_AND_INDEX() {
        return "Cannot force the query to pushdown and index at the same time. Only one of the parameter “forcedToPushDown“ and “forced_to_index” could be used. Please check and try again.";
    }

    public String getSECOND_STORAGE_NODE_NOT_AVAILABLE() {
        return "Can't add node. The node does not exist or has been used by other project, please modify and try again.";
    }

    public String getBASE_TABLE_INDEX_NOT_AVAILABLE() {
        return "Can’t turn on the tiered storage at the moment. Please add base table index first.";
    }

    public String getPARTITION_COLUMN_NOT_AVAILABLE() {
        return "Can’t turn on the tiered storage at the moment. Please add the time partition column as dimension, and update the base table index.";
    }

    public String getPROJECT_LOCKED() {
        return "There is tiered storage rebalance job running in progress, please try again later.";
    }

    public String getFIX_STREAMING_SEGMENT() {
        return "Can’t fix segment in streaming model.";
    }

    public String getStreamingDisabled() {
        return "The Real-time functions can only be used under Kyligence Premium Version, "
                + "please contact Kyligence customer manager to upgrade your license.";
    }

    public String getNO_STREAMING_MODEL_FOUND() {
        return "Can't be queried. As streaming data must be queried through indexes, please ensure there is an index for the query. ";
    }

    public String getSTREAMING_TABLE_NOT_SUPPORT_AUTO_MODELING() {
        return "No support streaming table for auto modeling.";
    }

    public String getSPARK_FAILURE() {
        return "Can't complete the operation. Please check the Spark environment and try again. ";
    }

    public String getDOWNLOAD_QUERY_HISTORY_TIMEOUT() {
        return "Export SQL timeout, please try again later.";
    }

    public String getSTREAMING_OPERATION_NOT_SUPPORT() {
        return "Can’t call this API. API calls related to the streaming data is not supported at the moment.";
    }

    public String getJDBC_CONNECTION_INFO_WRONG() {
        return "Invalid connection info.Please check and try again.";
    }

    public String getJDBC_NOT_SUPPORT_PARTITION_COLUMN_IN_SNAPSHOT() {
        return "Snapshot can’t use partition column for the current data source.";
    }

    public String getParamTooLarge() {
        return "The parameter '%s' is too large, maximum %s byte.";
    }

    // KAP query sql blacklist
    public String getSQL_BLACKLIST_ITEM_ID_EMPTY() {
        return "The id of blacklist item can not be empty.";
    }

    public String getSQL_BLACKLIST_ITEM_REGEX_AND_SQL_EMPTY() {
        return "The regex and sql of blacklist item can not all be empty.";
    }

    public String getSQL_BLACKLIST_ITEM_PROJECT_EMPTY() {
        return "The project of blacklist item can not be empty.";
    }

    public String getSQL_BLACKLIST_ITEM_ID_EXISTS() {
        return "Sql blacklist item id already exist.";
    }

    public String getSQL_BLACKLIST_ITEM_ID_NOT_EXISTS() {
        return "Sql blacklist item id not exists.";
    }

    public String getSQL_BLACKLIST_ITEM_REGEX_EXISTS() {
        return "Sql blacklist item regex already exist. Blacklist item id: %s .";
    }

    public String getSQL_BLACKLIST_ITEM_SQL_EXISTS() {
        return "Sql blacklist item sql already exist. Blacklist item id: %s .";
    }

    public String getSQL_BLACKLIST_ITEM_ID_TO_DELETE_EMPTY() {
        return "The id of sql blacklist item to delete can not be empty.";
    }

    public String getSQL_BLACKLIST_QUERY_REJECTED() {
        return "Query is rejected by blacklist, blacklist item id: %s.";
    }

    public String getSQL_BLACKLIST_QUERY_CONCUTTENT_LIMIT_EXCEEDED() {
        return "Query is rejected by blacklist because concurrent limit is exceeded, blacklist item id: %s, concurrent limit: {%s}";
    }

    public String getINVALID_RANGE() {
        return "%s is not integer in range [%s - %s] ";
    }

    public String getLDAP_USER_DATA_SOURCE_CONNECTION_FAILED() {
        return "The LDAP server is abnormal. Please check the user data source and try again.";
    }

    public String getLDAP_USER_DATA_SOURCE_CONFIG_ERROR() {
        return "LDAP connection error, please check LDAP configuration!";
    }

    public String getTABLE_NO_COLUMNS_PERMISSION() {
        return "Please add permissions to columns in the table!";
    }

    public String getPARAMETER_IS_REQUIRED() {
        return "'%s' is required.";
    }
}
