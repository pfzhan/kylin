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

import org.apache.kylin.common.Singletons;

import java.util.Locale;

public class Message {

    protected Message() {

    }

    public static Message getInstance() {
        return Singletons.getInstance(Message.class);
    }

    // Cube
    public String getCHECK_CC_AMBIGUITY() {
        return "In this model, computed column name [%s] has been used, please rename your computed column.";
    }

    public String getSEG_NOT_FOUND() {
        return "Cannot find segment '%s' in model '%s'.";
    }

    public String getACL_INFO_NOT_FOUND() {
        return "Unable to find ACL information for object identity '%s'.";
    }

    public String getACL_DOMAIN_NOT_FOUND() {
        return "Acl domain object required.";
    }

    public String getPARENT_ACL_NOT_FOUND() {
        return "Parent acl required.";
    }

    public String getIDENTITY_EXIST_CHILDREN() {
        return "Children exists for '%s'.";
    }

    // Model
    public String getINVALID_MODEL_DEFINITION() {
        return "The data model definition is invalid.";
    }

    public String getEMPTY_MODEL_NAME() {
        return "Model name should not be empty.";
    }

    public String getINIT_MEASURE_FAILED() {
        return "Cannot init measure %s: %s";
    }

    public String getINVALID_MODEL_NAME() {
        return "Invalid model name '%s', only letters, numbers and underlines are supported.";
    }

    public String getINVALID_DIMENSION_NAME() {
        return "The dimension name '%s' is invalid, only supports Chinese or English characters, numbers, spaces and symbol(_ -()%%?). %s characters at maximum.";
    }

    public String getINVALID_MEASURE_NAME() {
        return "The measure name '%s' is invalid, only supports Chinese or English characters, numbers, spaces and symbol(_ -()%%?). %s characters at maximum.";
    }

    public String getDUPLICATE_DIMENSION_NAME() {
        return "Dimension name '%s' already exists.";
    }

    public String getDUPLICATE_MEASURE_NAME() {
        return "Measure name '%s' already exists.";
    }

    public String getDUPLICATE_MEASURE_DEFINITION() {
        return "The measure definition is the same as the measure '%s'. So this measure cannot be created.";
    }

    public String getDUPLICATE_JOIN_CONDITIONS() {
        return "Duplicate join condition '%s' and '%s'.";
    }

    public String getDUPLICATE_MODEL_NAME() {
        return "Model name '%s' is duplicated, could not be created.";
    }

    public String getBROKEN_MODEL_OPERATION_DENIED() {
        return "BROKEN model \"%s\" cannot be operated.";
    }

    public String getMODEL_NOT_FOUND() {
        return "Data Model with name '%s' not found.";
    }

    public String getMODEL_MODIFY_ABANDON(String table) {
        return String.format(Locale.ROOT, "Model is not support to modify because you do not have permission of '%s'",
                table);
    }

    public String getEMPTY_PROJECT_NAME() {
        return "No valid project name. Please select one project.";
    }

    public String getGRANT_TABLE_WITH_SID_HAS_NOT_PROJECT_PERMISSION() {
        return "Failed to add table-level permissions.  User  (group)  [%s] has no project [%s] permissions.  Please grant user (group) project-level permissions first.";
    }

    public String getCheckCCType() {
        return "The actual data type of computed column {0} is {1}, but defined as {2}. Please modify and try again.";
    }

    public String getCheckCCExpression() {
        return "Failed to validate the expression '%2$s' in computed column '%1$s'.";
    }

    public String getMODEL_METADATA_PACKAGE_INVALID() {
        return "Parsing the file failed. Please check that the model package is complete.";
    }

    public String getEXPORT_BROKEN_MODEL() {
        return "Model [%s] is broken, can not export.";
    }

    public String getIMPORT_BROKEN_MODEL() {
        return "Model [%s] is broken, can not export.";
    }

    public String getIMPORT_MODEL_EXCEPTION() {
        return "Import model failed.";
    }

    public String getUN_SUITABLE_IMPORT_TYPE() {
        return "Model [%s]'s ImportType [%s] is illegal.";
    }

    public String getCAN_NOT_OVERWRITE_MODEL() {
        return "Model [%s] not exists, Can not overwrite model.";
    }

    public String getILLEGAL_MODEL_METADATA_FILE() {
        return "Please verify the metadata file first.";
    }

    public String getEXPORT_AT_LEAST_ONE_MODEL() {
        return "You should export one model at least.";
    }

    public String getCOMPUTED_COLUMN_EXPRESSION_DUPLICATED() {
        return "The expression of computed column has already been used in model '%s' as '%s'. Please modify the name to keep consistent, or use a different expression.";
    }

    public String getCOMPUTED_COLUMN_EXPRESSION_DUPLICATED_SINGLE_MODEL() {
        return "This expression has already been used by other computed columns in this model.";
    }

    public String getCOMPUTED_COLUMN_NAME_DUPLICATED() {
        return "The name of computed column '%s' has already been used in model '%s', and the expression is '%s'. Please modify the expression to keep consistent, or use a different name.";
    }

    public String getCOMPUTED_COLUMN_NAME_DUPLICATED_SINGLE_MODEL() {
        return "This name has already been used by other computed columns in this model.";
    }

    public String getMODEL_CHANGE_PERMISSION() {
        return "Only the system administrator or the project administrator can change the owner of the model.";
    }

    public String getMODEL_OWNER_CHANGE_INVALID_USER() {
        return "Illegal users! Only the system administrator, project administrator role, and management role can be set as the model owner.";
    }

    // index
    public String getINDEX_STATUS_TYPE_ERROR() {
        return "Parameter \"status\" only support NO_BUILD, ONLINE, LOCKED, BUILDING.";
    }

    public String getINDEX_SOURCE_TYPE_ERROR() {
        return "Parameter \"sources\" only support RECOMMENDED_AGG_INDEX, RECOMMENDED_TABLE_INDEX, CUSTOM_AGG_INDEX, CUSTOM_TABLE_INDEX.";
    }

    public String getINDEX_SORT_BY_ERROR() {
        return "Parameter \"sort_by\" only support last_modified, usage, data_size.";
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
        return "Illegal job state transfer, id: [%s], from: [%s], to: [%s]";
    }

    public String getINVALID_PRIORITY() {
        return "Invalid priority, must be range in 0-4";
    }

    // Acl
    public String getUSER_NOT_EXIST() {
        return "User '%s' does not exist. Please make sure the user exists.";
    }

    public String getUSERGROUP_NOT_EXIST() {
        return "Invalid values in parameter “group_name“. The value %s doesn’t exist.";
    }

    public String getUSERGROUP_EXIST() {
        return "Group [%s] already exists.";
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
        return "Invalid username or password.";
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
        return "User %s is locked, please try again after " + formatSeconds(leftSeconds) + ". "
                + formatNextLockDuration(nextLockSeconds);
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
        return formatTime(remainingDay, remainingHour, remainingMinutes, remainingSeconds);
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
        return "Can't find project \"%s\".";
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
        return "This project don't support multilevel partitioning, and multilevel partitioning can’t be used. Please turn it on in project setting and try again.";
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

    public String getQUERY_NOT_ALLOWED() {
        return "Job node is unavailable for queries. Please select a query node.";
    }

    public String getNOT_SUPPORTED_SQL() {
        return "Not Supported SQL.";
    }

    public String getQUERY_TOO_MANY_RUNNING() {
        return "Too many concurrent query requests.";
    }

    public String getEXPORT_RESULT_NOT_ALLOWED() {
        return "Current user is not allowed to export query result.";
    }

    public String getDUPLICATE_QUERY_NAME() {
        return "Duplicate query name '%s'";
    }

    public String getNULL_EMPTY_SQL() {
        return "SQL should not be empty.";
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

    // Async Query
    public String getQUERY_RESULT_NOT_FOUND() {
        return "The query corresponding to this query id in the current project cannot be found .";
    }

    public String getQUERY_RESULT_FILE_NOT_FOUND() {
        return "The query result file does not exist.";
    }

    public String getQUERY_EXCEPTION_FILE_NOT_FOUND() {
        return "The query exception file does not exist.";
    }

    public String getCLEAN_FOLDER_FAIL() {
        return "Failed to clean folder.";
    }

    public String getASYNC_QUERY_TIME_FORMAT_ERROR() {
        return "The time format is invalid. Please enter enter the date in the format yyyy-MM-dd HH:mm:ss.";
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

    public String getSQL_NUMBER_EXCEEDS_LIMIT() {
        return "Up to %s SQLs could be imported at a time";
    }

    public String getSQL_LIST_IS_EMPTY() {
        return "Please enter the array parameter \"sqls\".";
    }

    // License
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
                + "Please contact Kyligence, or try stopping some nodes and deleting some segments and stopping some nodes.";
    }

    public String getLICENSE_PROJECT_SOURCE_NODES_OVER_CAPACITY() {
        return "The amount of data volume used (%s/%s)  and nodes used (%s/%s) exceeds project’s limit.\n"
                + "Build index and load data is unavailable.\n"
                + "Please contact Kyligence, or try stopping some nodes and deleting some segments and stopping some nodes.";
    }

    public String getTABLENOTFOUND() {
        return "Failed to save model [%s]. Please ensure the columns used in model [%s] must be existed in source table [%s]";
    }

    // Async push down get date format
    public String getPUSHDOWN_PARTITIONFORMAT_ERROR() {
        return "Detect failed, please set the partition format manually.";
    }

    // Async push down get data range
    public String getPUSHDOWN_DATARANGE_ERROR() {
        return "Detect failed, please set the data range manually.";
    }

    public String getPUSHDOWN_DATARANGE_TIMEOUT() {
        return "Detect timeout, please set the data range manually.";
    }

    public String getDIMENSION_NOTFOUND() {
        return "The dimension %s is being referenced by aggregation group, recommended aggregate index or table index. Please delete this dimension from the above first.";
    }

    public String getMEASURE_NOTFOUND() {
        return "The measure %s is referenced by indexes. Please try again after deleting it from aggregation group or table index.";
    }

    public String getNESTED_CC_CASCADE_ERROR() {
        return "Operation failed. "
                + "There is a nested computed column [%s] in the current model depends on the current computed column [%s]. "
                + "The expression of nested computed column is [%s].";
    }

    public String getCHANGE_GLOBALADMIN() {
        return "You cannot add,modify or remove the system administrator’s rights";
    }

    public String getCHANGE_DEGAULTADMIN() {
        return "Since the user ADMIN is the default built-in administrator, you cannot remove the role admin permission, delete or disable the user admin, And only user ADMIN can change the password and user group of user ADMIN";
    }

    //Query
    public String getINVALID_USER_TAG() {
        return "user_defined_tag must be not greater than 256.";
    }

    public String getINVALID_ID() {
        return "Can’t find ID \"%s\". Please check and try again.";
    }

    public String getSEGMENT_LOCKED() {
        return "Can not remove or refresh or merge segment %s, because the segment is LOCKED.";
    }

    public String getSEGMENT_STATUS(String status) {
        return "Can not refresh or merge segment %s, because the segment is " + status + ".";
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
        return "Column [%s] is not dimension";
    }

    public String getMODEL_CAN_NOT_PURGE() {
        return "Model [%s] is table oriented, can not purge the model";
    }

    public String getMODEL_SEGMENT_CAN_NOT_REMOVE() {
        return "Model [%s] is table oriented, can not remove segments manually!";
    }

    public String getSEGMENT_CAN_NOT_REFRESH() {
        return "Can not refresh, some segments is building within the range you want to refresh!";
    }

    public String getSEGMENT_CAN_NOT_REFRESH_BY_SEGMENT_CHANGE() {
        return "Ready segments range has changed, can not refresh, please try again.";
    }

    public String getCAN_NOT_BUILD_SEGMENT() {
        return "Can not build segments, please define table index or aggregate index first!";
    }

    public String getCAN_NOT_BUILD_SEGMENT_MANUALLY() {
        return "Table oriented model [%s] can not build segments manually!";
    }

    public String getCAN_NOT_BUILD_INDICES_MANUALLY() {
        return "Table oriented model [%s] can not build indices manually!";
    }

    public String getINVALID_MERGE_SEGMENT() {
        return "Cannot merge segments which are not ready";
    }

    public String getINVALID_SET_TABLE_INC_LOADING() {
        return "Can not set table '%s' incremental loading, as another model '%s' uses it as a lookup table";
    }

    public String getINVALID_REFRESH_SEGMENT_BY_NO_SEGMENT() {
        return "There is no ready segment to refresh!";
    }

    public String getINVALID_REFRESH_SEGMENT_BY_NOT_READY() {
        return "Data during refresh range must be ready!";
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
        return "Segments id list can not empty!";
    }

    public String getSEGMENT_ID_NOT_EXIST() {
        return "Can not find the Segments by ids [%s].";
    }

    public String getSEGMENT_NAME_NOT_EXIST() {
        return "Can not find the Segments by names [%s].";
    }

    public String getLAYOUT_LIST_IS_EMPTY() {
        return "Layouts id list can not empty!";
    }

    public String getLAYOUT_NOT_EXISTS() {
        return "Layouts [%s] not exist!";
    }

    public String getINVALID_REFRESH_SEGMENT() {
        return "You should choose at least one segment to refresh!";
    }

    public String getEMPTY_SEGMENT_PARAMETER() {
        return "Please input the value of the Segment ID or Name.";
    }

    public String getCONFLICT_SEGMENT_PARAMETER() {
        return "You cannot input segment ID and Name at the same time.";
    }

    public String getINVALID_MERGE_SEGMENT_BY_TOO_LESS() {
        return "You should choose at least two segment to merge!";
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
        return "The computed column's name:[%s] is a sql keyword, please choose another name.";
    }

    public String getINVALID_COMPUTER_COLUMN_NAME() {
        return "The computed column's name:[%s] is invalid (null, start with number or underline, include symbol except char, "
                + "number and underline), please choose another name.";
    }

    public String getMODEL_ALIAS_DUPLICATED() {
        return "Model alias [%s] are duplicated!";
    }

    public String getINVALID_RANGE_LESS_THAN_ZERO() {
        return "Start or end of range must be greater than 0!";
    }

    public String getINVALID_RANGE_NOT_FORMAT() {
        return "Invalid start or end time format. Only support timestamp type, unit ms";
    }

    public String getINVALID_RANGE_END_LESSTHAN_START() {
        return "The end time must be greater than the start time";
    }

    public String getINVALID_RANGE_NOT_CONSISTENT() {
        return "Start and end must exist or not at the same time!";
    }

    public String getID_CANNOT_EMPTY() {
        return "Id cannot be empty";
    }

    public String getINVALID_CREATE_MODEL() {
        return "Can not create model manually in SQL acceleration project!";
    }

    public String getSEGMENT_INVALID_RANGE() {
        return "ToBeRefreshSegmentRange [%s] is out of range the coveredReadySegmentRange of dataLoadingRange, the coveredReadySegmentRange is [%s]";
    }

    public String getSEGMENT_RANGE_OVERLAP() {
        return "Segments to build overlaps built or building segment from %s to %s, please select new data range and try again!";
    }

    public String getPARTITION_COLUMN_NOT_EXIST() {
        return "Partition column does not exist!";
    }

    public String getINVALID_PARTITION_COLUMN() {
        return "Partition_date_column must use root fact table column";
    }

    public String getTABLE_NAME_CANNOT_EMPTY() {
        return "Table name must be specified!";
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
        return "Broken model [%s] can not online or offline!";
    }

    public String getINVALID_NAME_START_WITH_DOT() {
        return "The user / group names cannot start with a period (.).";
    }

    public String getINVALID_NAME_START_OR_END_WITH_BLANK() {
        return "User / group names cannot start or end with a space.";
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
        return "The same index already exists.";
    }

    public String getDEFAULT_REASON() {
        return "Something went wrong. %s";
    }

    public String getDEFAULT_SUGGEST() {
        return "Please contact Kyligence technical support for more details.";
    }

    public String getUNEXPECTED_TOKEN() {
        return "Syntax error: encountered unexpected token:\" %s\". At line %s, column %s.";
    }

    public String getBAD_SQL_REASON() {
        return "Syntax error:\"%s\".";
    }

    public String getBAD_SQL_SUGGEST() {
        return "Please correct the SQL.";
    }

    public String getBAD_SQL_TABLE_NOT_FOUND_REASON() {
        return "Table '%s' not found.";
    }

    public String getBAD_SQL_TABLE_NOT_FOUND_SUGGEST() {
        return "Please add table %s to data source. If this table does exist, mention it as DATABASE.TABLE.";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_REASON() {
        return "Column '%s' not found in any table.";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_SUGGEST() {
        return "Please add column %s to data source.";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_REASON() {
        return "The table\"%s\" doesn't include the column \"%s\".";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_SUGGESTION() {
        return "Please add column %s to table %s in data source.";
    }

    public String getPROJECT_NUM_OVER_THRESHOLD() {
        return "Failed to create a new project. The number of projects exceeds the maximum: {%s}. Please delete other abandoned projects before trying to create new ones or contact the administrator to adjust the maximum number of projects.";
    }

    public String getMODEL_NUM_OVER_THRESHOLD() {
        return "Failed to create a new model. The number of models exceeds the maximum: {%s}. Please delete other abandoned models before trying to create new ones or contact the administrator to adjust the maximum number of models.";
    }

    public String getQUERY_ROW_NUM_OVER_THRESHOLD() {
        return "Failed to get query result. The row number of query result exceeds the maximum: {%s}. Please add more filters or contact the administrator to adjust the maximum row number of query result.";
    }

    public String getCC_EXPRESSION_CONFLICT(String newCCExpression, String newCCName, String existedCCName) {
        return String.format(Locale.ROOT,
                "The expression %s of the computed column %s is the same as the computed column %s.", newCCExpression,
                newCCName, existedCCName);
    }

    public String getCC_NAME_CONFLICT(String ccName) {
        return String.format(Locale.ROOT, "Computed column \"%s\" already exists.", ccName);
    }

    public String get_ALIAS_CONFLICT_OF_APPROVING_RECOMMENDATION() {
        return "The name already exists. Please rename and try again.";
    }

    public String getDIMENSION_CONFLICT(String dimensionName) {
        return String.format(Locale.ROOT, "Dimension %s already exists.", dimensionName);
    }

    public String getMEASURE_CONFLICT(String measureName) {
        return String.format(Locale.ROOT, "Measure %s already exists.", measureName);
    }

    public String getJOB_NODE_INVALID(String url) {
        return "Can’t execute this request on job node. Please check and try again.";
    }

    public String getINVALID_TIME_FORMAT() {
        return "Can’t set the time partition column. The values of the selected column is not time formatted. Please select again.";
    }

    public String getSEGMENT_CONTAINS_GAPS() {
        return "The range of segments %s and %s are discontinuous.";
    }

    public String getSegmentMergeLayoutConflictError() {
        return "The indexes included in the selected segments are not fully identical. Please build index first and try merging again.";
    }

    public String getSegmentMergePartitionConflictError() {
        return "The partitions included in the selected segments are not fully aligned. Please build partition first and try merging again.";
    }

    public String getDIMENSION_TABLE_USED_IN_THIS_MODEL() {
        return "The dimension table of this model has been used as fact table. Please set another dimension table.";
    }

    public String getNO_DATA_IN_TABLE() {
        return "There are no data in table %s.";
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
        return "Failed to rewrite the model settings, %s parameter value must be non-negative integer.";
    }

    public String getINVALID_MEMORY_SIZE() {
        return "Failed to rewrite the model settings, spark-conf.spark.executor.instances parameter "
                + "value must be a combination of non-negative integer and unit g.";
    }

    public String getINVALID_BOOLEAN_FORMAT() {
        return "Failed to rewrite the model settings, %s parameter value must be true or false.";
    }

    public String getINVALID_AUTO_MERGE_CONFIG() {
        return "Failed to rewrite model settings, automatic merge range cannot be empty.";
    }

    public String getINVALID_VOLATILE_RANGE_CONFIG() {
        return "Failed to rewrite the model setting, the unit of the dynamic interval parameter must be "
                + "one of day, week, month, and year and the value must be non-negative integer.";
    }

    public String getINVALID_RETENTION_RANGE_CONFIG() {
        return "Failed to rewrite the model settings, parameter value must be non-negative integer and the "
                + "unit of parameter must be the coarsest granularity unit in the unit selected for automatic merge.";
    }

    public String getINSUFFICIENT_AUTHENTICATION() {
        return "Unable to authenticate. Please login again.";
    }

    public String getWRITE_IN_MAINTENANCE_MODE() {
        return "System is currently undergoing maintenance. Metadata related operations are temporarily unavailable.";
    }

    public String getINVALID_SID_TYPE() {
        return "Invalid value for parameter ‘sid_type’ which should only be ‘user’ or ‘group’(case-insensitive).";
    }

    public String getEMPTY_DATABASE_NAME() {
        return "Invalid value for parameter ‘database_name’ which should not be empty.";
    }

    public String getEMPTY_DATABASE() {
        return "Invalid value for parameter ‘database’ which should not be empty.";
    }

    public String getEMPTY_TABLE_LIST() {
        return "Invalid value for parameter ‘tables’ which should not be empty.";
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
        return String.format(Locale.ROOT, "Column [%s] assignment is failed.Please check the column date type.",
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
        return "The job failed to be submitted. There already exists building job running under the corresponding subject.";
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

    public String getADD_JOB_CHECK_MULTI_PARTITION_ABANDON() {
        return "Add Job failed due to multi partition param is illegal.";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_EMPTY() {
        return "Add Job failed due to multi partition value is empty.";
    }

    public String getADD_JOB_CHECK_MULTI_PARTITION_DUPLICATE(String dupPartitions) {
        return String.format(Locale.ROOT, "Add Job failed due to partition [%s] is duplicated.", dupPartitions);
    }

    public String getTABLE_RELOAD_ADD_COLUMN_EXIST(String table, String column) {
        return String.format(Locale.ROOT,
                "The table metadata can’t be reloaded now. Column %s already exists in table %s.", column, table);
    }

    public String getTABLE_RELOAD_HAVING_NOT_FINAL_JOB() {
        return "The table metadata can’t be reloaded now. There are ongoing jobs with the following target subjects(s): %s. Please try reloading until all the jobs are completed, or manually discard the jobs.";
    }

    public String getCOLUMN_UNRECOGNIZED() {
        return "Cannot recognize column(s) %s . When referencing a column, please follow the format as TABLE_ALIAS.COLUMN (TABLE_ALIAS is the table name defined in the model).";
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
        return "Group uuid %s is not exist.";
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
        return "Failed to connect database.Please check if the database connection is healthy.";
    }

    // acl
    public String getInvalidColumnAccess() {
        return "The current user/group does not have permission of column %s.";
    }

    public String getInvalidSensitiveDataMaskColumnType() {
        return "Boolean, Map, and Array data types do not support data masking.";
    }

    public String getNotSupportNestedDependentCol() {
        return "Not Supported setting association rules on association columns [%s].";
    }

    // Snapshots
    public String getSNAPSHOT_OPERATION_PERMISSION_DENIED() {
        return "Have no permission for table(s) '%s'.";
    }

    public String getSNAPSHOT_NOT_FOUND() {
        return "Snapshot(s) '%s' not found.";
    }

    public String getSNAPSHOT_MANAGEMENT_NOT_ENABLED() {
        return "Snapshot management is not enabled.";
    }

    public String getINVALID_DIAG_TIME_PARAMETER() {
        return "The end time must be greater than the start time, please input the correct parameters to execute.";
    }

    // Resource Group
    public String getRESOURCE_GROUP_FIELD_IS_NULL() {
        return "Resource group fields can not be null.";
    }

    public String getRESOURCE_CAN_NOT_BE_EMPTY() {
        return "In resource group mode, at least one resource group exists.";
    }

    public String getEMPTY_RESOURCE_GROUP_ID() {
        return "Resource group id can not be empty.";
    }

    public String getDUPLICATED_RESOURCE_GROUP_ID() {
        return "Resource group id can not duplicated.";
    }

    public String getRESOURCE_GROUP_DISABLED_WITH_INVLIAD_PARAM() {
        return "Resource group information must be cleared before closing the resource group.";
    }

    public String getPROJECT_WITHOUT_RESOURCE_GROUP() {
        return "No Resource Group is allocated, please contact your administrator.";
    }

    public String getEMPTY_KYLIN_INSTANCE_IDENTITY() {
        return "Kylin instance filed can not be empty.";
    }

    public String getEMPTY_KYLIN_INSTANCE_RESOURCE_GROUP_ID() {
        return "Kylin instance resource_group_id field can not be empty.";
    }

    public String getRESOURCE_GROUP_ID_NOT_EXIST_IN_KYLIN_INSTANCE() {
        return "Kylin instance resource_group_id not exists.";
    }

    public String getDUPLICATED_KYLIN_INSTANCE() {
        return "Kylin instance can not duplicated.";
    }

    public String getEMPTY_PROJECT_IN_MAPPING_INFO() {
        return "Project can not be empty in mapping_info.";
    }

    public String getEMPTY_RESOURCE_GROUP_ID_IN_MAPPING_INFO() {
        return "Resource group id can not be empty in mapping_info.";
    }

    public String getPROJECT_BINDING_RESOURCE_GROUP_INVALID() {
        return "A project is bound to two resource groups at most, and each request is bound to at most one resource group, invalid project name: [%s].";
    }

    public String getRESOURCE_GROUP_ID_NOT_EXIST_IN_MAPPING_INFO() {
        return "Mapping info resource_group_id not exists.";
    }

    public String getMODEL_ONLINE_FORBIDDEN() {
        return "This model can’t go online.";
    }

    // multi level partition mapping
    public String getMULTI_PARTITION_MAPPING_REQEUST_NOT_VALID() {
        return "Failed to update multi-partition mapping, invalid request";
    }

    public String getCONCURRENT_SUBMIT_JOB_LIMIT() {
        return "Can't submit building jobs, as it exceeds the concurrency limit (%s).  Please try submitting fewer jobs at a time.";
    }

    public String getMODEL_IS_NOT_MLP() {
        return "Model '%s' does not have sub partition.";
    }

    public String getINVALID_PARTITION_VALUE() {
        return "The subpartition(s) “%s“ doesn’t exist. Please check and try again.";
    }

    public String getPARTITION_VALUE_NOT_SUPPORT() {
        return "The subpartition column of model '%s' has not been set yet. Please set it first.";
    }

    public String getADMIN_PERMISSION_UPDATE_ABANDON() {
        return "Admin is not supported to update permission.";
    }

    public String getMODEL_ID_NOT_EXIST() {
        return "Data model with id '%s' not found.";
    }

    public String getNot_IN_EFFECTIVE_COLLECTION() {
        return "%s not in effective collection : %s.";
    }

    public String getROW_ACL_NOT_STRING_TYPE() {
        return "The like operator could only be used for char or varchar data type. Please reset.";
    }

    public String getSTOP_BY_USER_ERROR_MESSAGE() {
        return "Stopped by user.";
    }

    public String getExceedMaxAllowedPacket() {
        return "The result packet of MySQL exceeds the limit. Please contact the admin to adjust the value of “max_allowed_packet“ as 256M in MySQL. ";
    }
}
