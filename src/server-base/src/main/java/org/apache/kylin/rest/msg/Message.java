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

package org.apache.kylin.rest.msg;

import org.apache.kylin.metadata.model.SegmentStatusEnumToDisplay;

public class Message {

    private static Message instance = null;

    protected Message() {

    }

    public static Message getInstance() {
        if (instance == null) {
            instance = new Message();
        }
        return instance;
    }

    // Cube
    public String getCHECK_CC_AMBIGUITY() {
        return "In this model, computed column name [%s] has been used, please rename your computed column.";
    }

    public String getCUBE_NOT_FOUND() {
        return "Cannot find cube '%s'.";
    }

    public String getSEG_NOT_FOUND() {
        return "Cannot find segment '%s' in model '%s'.";
    }

    public String getKAFKA_DEP_NOT_FOUND() {
        return "Could not find Kafka dependency.";
    }

    public String getBUILD_DRAFT_CUBE() {
        return "Could not build draft cube.";
    }

    public String getBUILD_BROKEN_CUBE() {
        return "Broken cube '%s' can't be built.";
    }

    public String getINCONSISTENT_CUBE_DESC_SIGNATURE() {
        return "Inconsistent cube desc signature for '%s', if it's right after an upgrade, please try 'Edit CubeDesc' to delete the 'signature' field. Or use 'bin/metastore.sh refresh-cube-signature' to batch refresh all cubes' signatures, then reload metadata to take effect.";
    }

    public String getDELETE_NOT_READY_SEG() {
        return "Cannot delete segment '%s' as its status is not READY. Discard the on-going job for it.";
    }

    public String getINVALID_BUILD_TYPE() {
        return "Invalid build type: '%s'.";
    }

    public String getNO_ACL_ENTRY() {
        return "There should have been an Acl entry for ObjectIdentity '%s'.";
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

    public String getDISABLE_NOT_READY_CUBE() {
        return "Only ready cube can be disabled, status of '%s' is %s.";
    }

    public String getPURGE_NOT_DISABLED_CUBE() {
        return "Only disabled cube can be purged, status of '%s' is %s.";
    }

    public String getCLONE_BROKEN_CUBE() {
        return "Broken cube '%s' can't be cloned.";
    }

    public String getINVALID_CUBE_NAME() {
        return "Invalid Cube name '%s', only letters, numbers and underlines are supported.";
    }

    public String getCUBE_ALREADY_EXIST() {
        return "The cube named '%s' already exists.";
    }

    public String getCUBE_DESC_ALREADY_EXIST() {
        return "The cube desc named '%s' already exists.";
    }

    public String getBROKEN_CUBE_DESC() {
        return "Broken cube desc named '%s'.";
    }

    public String getENABLE_NOT_DISABLED_CUBE() {
        return "Only disabled cube can be enabled, status of '%s' is %s.";
    }

    public String getNO_READY_SEGMENT() {
        return "Cube '%s' doesn't contain any READY segment.";
    }

    public String getDELETE_SEGMENT_CAUSE_GAPS() {
        return "Cube '%s' has gaps caused by deleting segment '%s'.";
    }

    public String getENABLE_WITH_RUNNING_JOB() {
        return "Enable is not allowed with a running job.";
    }

    public String getDISCARD_JOB_FIRST() {
        return "The cube '%s' has running or failed job, please discard it and try again.";
    }

    public String getIDENTITY_EXIST_CHILDREN() {
        return "Children exists for '%s'.";
    }

    public String getINVALID_CUBE_DEFINITION() {
        return "The cube definition is invalid.";
    }

    public String getEMPTY_CUBE_NAME() {
        return "Cube name should not be empty.";
    }

    public String getUSE_DRAFT_MODEL() {
        return "Cannot use draft model '%s'.";
    }

    public String getINCONSISTENT_CUBE_DESC() {
        return "CubeDesc '%s' is inconsistent with existing. Try purge that cube first or avoid updating key cube desc fields.";
    }

    public String getUPDATE_CUBE_NO_RIGHT() {
        return "You don't have right to update this cube.";
    }

    public String getNOT_STREAMING_CUBE() {
        return "Cube '%s' is not a Streaming Cube.";
    }

    public String getCUBE_RENAME() {
        return "Cube renaming is not allowed.";
    }

    // Model
    public String getINVALID_MODEL_DEFINITION() {
        return "The data model definition is invalid.";
    }

    public String getEMPTY_MODEL_NAME() {
        return "Model name should not be empty.";
    }

    public String getINVALID_MODEL_NAME() {
        return "Invalid model name '%s', only letters, numbers and underlines are supported.";
    }

    public String getINVALID_DIMENSION_NAME() {
        return "Invalid dimension name '%s', only letters, numbers and underlines are supported.";
    }

    public String getINVALID_MEASURE_NAME() {
        return "Invalid measure name '%s', only letters, numbers and underlines are supported.";
    }

    public String getMODEL_ID_NOT_FOUND() {
        return "Model id required";
    }

    public String getINDEX_ALREADY_DELETED() {
        return "The index has been deleted.";
    }

    public String getDUPLICATE_DIMENSION_NAME() {
        return "Duplicate dimension name '%s'.";
    }

    public String getDUPLICATE_MEASURE_NAME() {
        return "Duplicate measure name '%s'.";
    }

    public String getDUPLICATE_MEASURE_DEFINITION() {
        return "Duplicate measure definition '%s'.";
    }

    public String getDUPLICATE_JOIN_CONDITIONS() {
        return "Duplicate join condition '%s' and '%s'.";
    }

    public String getDUPLICATE_MODEL_NAME() {
        return "Model name '%s' is duplicated, could not be created.";
    }

    public String getDROP_REFERENCED_MODEL() {
        return "Model is referenced by IndexPlan '%s' , could not dropped";
    }

    public String getUPDATE_MODEL_KEY_FIELD() {
        return "Model cannot save because there are dimensions, measures or join relations modified to be inconsistent with existing cube.";
    }

    public String getBROKEN_MODEL_DESC() {
        return "Broken model desc named '%s'.";
    }

    public String getMODEL_NOT_FOUND() {
        return "Data Model with name '%s' not found.";
    }

    public String getEMPTY_PROJECT_NAME() {
        return "No valid project name. Please select one project.";
    }

    public String getEMPTY_NEW_MODEL_NAME() {
        return "New model name should not be empty.";
    }

    public String getUPDATE_MODEL_NO_RIGHT() {
        return "You don't have right to update this model.";
    }

    public String getMODEL_RENAME() {
        return "Model renaming is not allowed.";
    }

    public String getCheckCCType() {
        return "The actual data type of computed column {0} is {1}, but defined as {2}. Please modify and try again.";
    }

    public String getMODEL_METADATA_PACKAGE_INVALID() {
        return "Parsing the file failed. Please check that the model package is complete.";
    }

    public String getCOMPUTED_COLUMN_EXPRESSION_ALREADY_DEFINED() {
        return "Model '%s' import failed: Computed column '%s' of this model has the same expression as model '%s' computed column '%s'.";
    }

    public String getCOMPUTED_COLUMN_NAME_ALREADY_DEFINED() {
        return "Model '%s' import failed: Computed column '%s' of this model has the same name as computed column in model '%s'.";
    }

    public String getFACT_TABLE_USED_AS_LOOK_UP_TABLE() {
        return "Model '%s' import failed: the fact table in the model already exists as a dimension table in other models.";
    }

    // Job
    public String getILLEGAL_TIME_FILTER() {
        return "Illegal timeFilter: %s.";
    }

    public String getILLEGAL_EXECUTABLE_STATE() {
        return "Illegal executable status: %s.";
    }

    public String getILLEGAL_JOB_STATE() {
        return "Illegal job status: %s.";
    }

    public String getILLEGAL_JOB_TYPE() {
        return "Illegal job type, id: %s.";
    }

    // Acl
    public String getUSER_NOT_EXIST() {
        return "User '%s' does not exist. Please make sure the user exists.";
    }

    // user group
    public String getEMPTY_GROUP_NAME() {
        return "User group name should not be empty.";
    }

    public String getEMPTY_SID() {
        return "User/Group name should not be empty.";
    }

    public String getINVALID_SID() {
        return "User/Group name should only contain alphanumerics, spaces and underscores.";
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
        return "User editing is not allowed unless in testing profile, please go to LDAP/SAML provider instead";
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

    public String getUSER_BE_LOCKED() {
        return "Invalid username or password. Please try again after 30 seconds.";
    }

    public String getUSER_IN_LOCKED_STATUS() {
        return "User %s is locked, please try again after %s seconds.";
    }

    public String getUSER_LOGIN_AS_USER_NOT_ADMIN() {
        return "Only ADMIN user is allowed to login in as another user.";
    }

    // Project
    public String getINVALID_PROJECT_NAME() {
        return "Please use number, letter, and underline to name your project, and start with a number or a letter.";
    }

    public String getPROJECT_ALREADY_EXIST() {
        return "The project named '%s' already exists.";
    }

    public String sourceTYPE_NOT_ALLOWED() {
        return "this source type not allowed";
    }

    public String getPROJECT_NOT_FOUND() {
        return "Cannot find project '%s'.";
    }

    public String getPROJECT_UNMODIFIABLE_REASON() {
        return "Model recommendation is not supported in current project, please enable recommendation mode and try again.";
    }

    public String getPROJECT_MODE_CHANGE_ERROR() {
        return "Recommendation conversion is in progress. This process may take few minutes, please turn off recommendations mode later.";
    }

    public String getDELETE_PROJECT_NOT_EMPTY() {
        return "Cannot modify non-empty project";
    }

    public String getPROJECT_RENAME() {
        return "Project renaming is not allowed.";
    }

    public String getPROJECT_FAILED() {
        return "Cannot find project.";
    }

    // Table
    public String getTABLE_NOT_FOUND() {
        return "Cannot find table '%s'.";
    }

    public String getTABLE_DESC_NOT_FOUND() {
        return "Cannot find table descriptor '%s'.";
    }

    public String getTABLE_IN_USE_BY_MODEL() {
        return "Table is already in use by models '%s'.";
    }

    public String getBEYOND_MIX_SAMPLING_ROWSHINT() {
        return "Sampling range should not be less than the max limit(%s rows)!";
    }

    public String getBEYOND_MAX_SAMPLING_ROWS_HINT() {
        return "Sampling range should not exceed the max limit(%s rows)!";
    }

    public String getSAMPLING_FAILED_FOR_ILLEGAL_TABLE_NAME() {
        return "Illegal table name '%s', please input a qualified table name as database.table!";
    }

    public String getFAILED_FOR_IN_SAMPLING() {
        return "The source table %s has a related sample job running. Please trigger another sample job later.";
    }

    public String getFAILED_FOR_NO_SAMPLING_TABLE() {
        return "Please input at least one table(database.table) for sampling!";
    }

    // Cube Desc
    public String getCUBE_DESC_NOT_FOUND() {
        return "Cannot find cube desc '%s'.";
    }

    // Streaming
    public String getINVALID_TABLE_DESC_DEFINITION() {
        return "The TableDesc definition is invalid.";
    }

    public String getINVALID_STREAMING_CONFIG_DEFINITION() {
        return "The StreamingConfig definition is invalid.";
    }

    public String getINVALID_KAFKA_CONFIG_DEFINITION() {
        return "The KafkaConfig definition is invalid.";
    }

    public String getADD_STREAMING_TABLE_FAIL() {
        return "Failed to add streaming table.";
    }

    public String getEMPTY_STREAMING_CONFIG_NAME() {
        return "StreamingConfig name should not be empty.";
    }

    public String getSTREAMING_CONFIG_ALREADY_EXIST() {
        return "The streamingConfig named '%s' already exists.";
    }

    public String getSAVE_STREAMING_CONFIG_FAIL() {
        return "Failed to save StreamingConfig.";
    }

    public String getKAFKA_CONFIG_ALREADY_EXIST() {
        return "The kafkaConfig named '%s' already exists.";
    }

    public String getCREATE_KAFKA_CONFIG_FAIL() {
        return "StreamingConfig is created, but failed to create KafkaConfig.";
    }

    public String getSAVE_KAFKA_CONFIG_FAIL() {
        return "Failed to save KafkaConfig.";
    }

    public String getROLLBACK_STREAMING_CONFIG_FAIL() {
        return "Action failed and failed to rollback the created streaming config.";
    }

    public String getROLLBACK_KAFKA_CONFIG_FAIL() {
        return "Action failed and failed to rollback the created kafka config.";
    }

    public String getUPDATE_STREAMING_CONFIG_NO_RIGHT() {
        return "You don't have right to update this StreamingConfig.";
    }

    public String getUPDATE_KAFKA_CONFIG_NO_RIGHT() {
        return "You don't have right to update this KafkaConfig.";
    }

    public String getSTREAMING_CONFIG_NOT_FOUND() {
        return "StreamingConfig with name '%s' not found.";
    }

    // Query
    public String getQUERY_NOT_ALLOWED() {
        return "Query is not allowed in '%s' mode.";
    }

    public String getNOT_SUPPORTED_SQL() {
        return "Not Supported SQL.";
    }

    public String getQUERY_TOO_MANY_RUNNING() {
        return "Too many concurrent query requests.";
    }

    public String getTABLE_META_INCONSISTENT() {
        return "Table metadata inconsistent with JDBC meta.";
    }

    public String getCOLUMN_META_INCONSISTENT() {
        return "Column metadata inconsistent with JDBC meta.";
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

    public String getACE_ID_REQUIRED() {
        return "Ace id required.";
    }

    // Async Query
    public String getQUERY_RESULT_NOT_FOUND() {
        return "Result for this query is unavailable, please check its status first.";
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

    // Admin
    public String getGET_ENV_CONFIG_FAIL() {
        return "Failed to get Kylin env Config.";
    }

    // User
    public String getAUTH_INFO_NOT_FOUND() {
        return "Cannot find authentication information.";
    }

    public String getUSER_NOT_FOUND() {
        return "User '%s' not found.";
    }

    // Diagnosis
    public String getDIAG_NOT_FOUND() {
        return "diag.sh not found at %s.";
    }

    public String getGENERATE_DIAG_PACKAGE_FAIL() {
        return "Failed to generate diagnostic package.";
    }

    public String getDIAG_PACKAGE_NOT_AVAILABLE() {
        return "Diagnostic package is not available in directory: %s.";
    }

    public String getDIAG_PACKAGE_NOT_FOUND() {
        return "Diagnostic package not found in directory: %s.";
    }

    // Encoding
    public String getVALID_ENCODING_NOT_AVAILABLE() {
        return "Can not provide valid encodings for datatype: %s.";
    }

    // ExternalFilter
    public String getFILTER_ALREADY_EXIST() {
        return "The filter named '%s' already exists.";
    }

    public String getFILTER_NOT_FOUND() {
        return "The filter named '%s' does not exist.";
    }

    // Basic
    public String getHBASE_FAIL() {
        return "HBase failed: '%s'";
    }

    public String getHBASE_FAIL_WITHOUT_DETAIL() {
        return "HBase failed.";
    }

    // Favorite Query
    public String getFAVORITE_RULE_NOT_FOUND() {
        return "Cannot find favorite rule '%s' ";
    }

    public String getUNACCELERATE_FAVORITE_QUERIES_NOT_ENOUGH() {
        return "Current number of unAccelerated favorite queries has not reached to '%s' ";
    }

    public String getFAIL_TO_VERIFY_SQL() {
        return "Failed to verify SQLs.";
    }

    public String getFAVORITE_QUERY_NOT_EXIST() {
        return "Favorite query '%s' does not exist";
    }

    // Query statistics

    public String getNOT_SET_INFLUXDB() {
        return "Not set kap.metric.write-destination to 'INFLUX'";
    }

    // License

    public String getLICENSE_OVERDUE_TRIAL() {
        return "The license has expired and the validity period is [%s - %s]. Please upload a new license or contact the Kyligence sales staff.";
    }

    public String getLICENSE_NODES_EXCEED() {
        return "The number of nodes which you are using is higher than the allowable number. Please contact your Kyligence account manager.";
    }

    public String getLICENSE_WRONG_CATEGORY() {
        return "The current version of Kyligence Enterprise does not match the license. Please upload a new license or contact the Kyligence sales staff.";
    }

    public String getLICENSE_NO_LICENSE() {
        return "No license file. Please contact the Kyligence sales staff.";
    }

    public String getLICENSE_INVALID_LICENSE() {
        return "Invalid license, please contact the Kyligence sales staff.";
    }

    public String getLICENSE_MISMATCH_LICENSE() {
        return "The cluster information does not match the license. Please upload a new license or contact the Kyligence sales staff.";
    }

    public String getLICENSE_NOT_EFFECTIVE() {
        return "License is not effective yet, please apply for a new license.";
    }

    public String getLICENSE_EXPIRED() {
        return "This license has expired and the validity period is [%s - %s]. Please contact the Kyligence sales staff.";
    }

    public String getINVALID_CREDENTIAL() {
        return "Invalid credential.Please check your credential";
    }

    public String getINVALID_URL() {
        return "Invalid object url.Please check your object url";
    }

    public String getNoTableFound() {
        return "Can not find any table.Please check your sql";
    }

    public String getNoJobNode() {
        return "No job node(or all node) detected. Please start the node or configure a job node( or all node) via the property \"kylin.server.mode\" in the file ${KYLIN_HOME}/conf/kylin.properties.";
    }

    public String getTABLENOTFOUND() {
        return "Failed to save model [%s]. Please ensure the columns used in model [%s] must be existed in source table [%s]";
    }

    // Async push down get data range
    public String getPUSHDOWN_DATARANGE_ERROR() {
        return "Detect failed, please set the data range manually.";
    }

    public String getPUSHDOWN_DATARANGE_TIMEOUT() {
        return "Detect timeout, please set the data range manually.";
    }

    public String getAGGINDEX_DIMENSION_NOTFOUND() {
        return "model %s's agg group still contains dimension(s) %s";
    }

    public String getTABLEINDEX_COLUMN_NOTFOUND() {
        return "model %s's table index still contains column(s) %s";
    }

    public String getAGGINDEX_MEASURE_NOTFOUND() {
        return "model %s's agg group still contains measure(s) %s";
    }

    public String getDOWNLOAD_FAILED() {
        return "Failed to download file.";
    }

    public String getDIAG_PACKAGE_TIMEOUT() {
        return "Package generating timeout, please contact Kyligence Support for help.";
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
        return "id {%s} is not exist";
    }

    public String getSEGMENT_LOCKED() {
        return "Can not remove or refresh or merge segment [%s], because the segment is LOCKED.";
    }

    public String getSEGMENT_STATUS(SegmentStatusEnumToDisplay status) {
        return "Can not refresh or merge segment [%s], because the segment is " + status + ".";
    }

    //Kerberos
    public String getKERBEROS_INFO_ERROR() {
        return "Invalid principle name or keytab file, please check and submit again.";
    }

    public String getPROJECT_HIVE_PERMISSION_ERROR() {
        return "Permission denied. Please confirm the Kerberos account can access all the loaded tables.";
    }
}