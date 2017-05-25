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

package io.kyligence.kap.rest.msg;

/**
 * Created by luwei on 17-5-10.
 */
public class KapMessage {

    private static KapMessage instance = null;

    protected KapMessage() {

    }

    public static KapMessage getInstance() {
        if (instance == null) {
            instance = new KapMessage();
        }
        return instance;
    }

    public String gettestpattern() {
        return "kap: %s must be %d";
    }

    // KAP Async Query
    private final String CLEAN_FOLDER_FAIL = "Failed to clean folder.";
    private final String QUERY_EXCEPTION_NOT_FOUND = "No exception for this query is available, please check its status first.";
    private final String QUERY_EXCEPTION_FILE_NOT_FOUND = "The query exception file does not exist.";
    private final String QUERY_RESULT_NOT_FOUND = "No result for this query is available, please check its status first.";
    private final String QUERY_RESULT_FILE_NOT_FOUND = "The query result file does not exist.";

    // Cube
    private final String CUBE_NOT_FOUND = "Cannot find cube '%s'.";
    private final String UPDATE_CUBE_NO_RIGHT = "You don't have right to update this cube.";
    private final String INVALID_CUBE_DEFINITION = "The cube definition is invalid.";
    private final String DISCARD_JOB_FIRST = "The cube '%s' has running or failed job, please discard it and try again.";

    // KAP Cube
    private final String RAW_SEG_SIZE_NOT_ONE = "There should be exactly one rawtable segment.";

    // KAP User
    private final String EMPTY_USER_NAME = "User name should not be empty.";
    private final String SHORT_PASSWORD = "The password should contain more than 8 characters!";
    private final String INVALID_PASSWORD = "The password should contain at least one numbers, letters and special characters（~!@#$%^&*(){}|:\"<>?[];\\'\\,./`).";
    private final String PERMISSION_DENIED = "Permission denied!";
    private final String OLD_PASSWORD_WRONG = "Old password is not correct!";

    // KAP Raw Table
    private final String INVALID_RAWTABLE_DEFINITION = "The rawtable definition is not valid.";
    private final String EMPTY_RAWTABLE_NAME = "RawTable name should not be empty.";
    private final String RAWTABLE_ALREADY_EXIST = "The raw table named '%s' already exists.";
    private final String RAW_DESC_ALREADY_EXIST = "The raw desc named '%s' already exists.";
    private final String RAWTABLE_NOT_FOUND = "The raw table named '%s' does not exist.";
    private final String RAW_DESC_RENAME = "Raw Desc renaming is not allowed: new: '%s', origin: '%s'.";
    private final String RAWTABLE_SCHEMA_CHANGE_WITH_RUNNING_JOB = "RawTable schema shouldn't be changed with running job.";
    private final String RAWTABLE_HAS_RUNNING_JOB = "The raw '%s' has running job, please discard it and try again.";
    private final String ENABLE_NOT_DISABLED_RAWTABLE = "Only disabled raw table can be enabled, status of '%s' is '%s'.";
    private final String RAWTABLE_NO_READY_SEGMENT = "Raw table '%s' doesn't contain any READY segment.";
    private final String RAWTABLE_ENABLE_WITH_RUNNING_JOB = "Enable is not allowed with a running job.";
    private final String DISABLE_NOT_READY_RAWTABLE = "Only ready raw table can be disabled, status of '%s' is '%s'.";
    private final String RAWTABLE_DESC_NOT_FOUND = "The raw table desc named '%s' does not exist.";
    private final String UNEXPECTED_RAWTABLE_DESC_STATUS = "Raw table status should not be '%s'.";
    private final String NO_DRAFT_RAWTABLE_TO_UPDATE = "Raw table '%s' has no draft to update.";
    private final String NON_DRAFT_RAWTABLE_ALREADY_EXIST = "A non-draft raw table with name '%s' already exists.";
    private final String RAWTABLE_RENAME = "Raw table renaming is not allowed.";
    private final String ORIGIN_RAWTABLE_NOT_FOUND = "Origin raw table not found.";

    // KAP Scheduler job
    private final String SCHEDULER_JOB_NOT_FOUND = "Cannot find scheduler job '%s'.";

    // KAP Sequence SQL
    private final String UNIQUE_SEQ_ID_REQUIRED = "Must provided a unique sequenceID for a SQL sequence.";
    private final String STEPID_NOT_DEFAULT = "If you're not updating a certain sql, you should leave stepID as default (-1).";
    private final String SQL_AND_RESULT_OPT_REQUIRED = "Sql and result opt are required.";
    private final String SQL_REQUIRED = "Sql is required.";
    private final String EXISTING_STEPID_REQUIRED = "If you're updating a certain sql, you should provide an existing stepID.";
    private final String UPDATE_STEP0_RESULT_OPT = "Result opt cannot be updated for step 0.";
    private final String SQL_OR_RESULT_OPT_REQUIRED = "sql or result opt are required.";
    private final String ONE_SHARED_RESULT_NULL = "One of the shard result is null.";
    private final String ONE_SHARED_EXCEPTION = "One of the shard met exception: '%s'.";
    private final String SEQ_TOPOLOGY_NOT_FOUND = "The sequence topology is not found, maybe expired?";
    private final String TOPOLOGY_FINAL_RESULT_NOT_FOUND = "The final result for current topology is not found!";

    // Query
    private final String QUERY_NOT_ALLOWED = "Query is not allowed in '%s' mode.";
    private final String NOT_SUPPORTED_SQL = "Not Supported SQL.";

    // KAP Config
    private final String EMPTY_FEATURE_NAME = "Request field feature_name cannot be empty.";

    // Diagnosis
    private final String DIAG_NOT_FOUND = "diag.sh not found at %s.";

    // KAP KyBot
    private final String GENERATE_KYBOT_PACKAGE_FAIL = "Failed to generate KyBot package.";
    private final String KYBOT_PACKAGE_NOT_AVAILABLE = "KyBot package is not available in directory: '%s'.";
    private final String KYBOT_PACKAGE_NOT_FOUND = "KyBot package not found in directory: '%s'.";
    private final String DUMP_KYBOT_PACKAGE_FAIL = "Failed to dump kybot package.";
    private final String USERNAME_OR_PASSWORD_EMPTY = "Either username or password should not be empty.";
    private final String AUTH_FAIL = "Authentication failed.";

    // KAP Metastore
    private final String KYLIN_HOME_UNDEFINED = "KYLIN_HOME undefined.";
    private final String METADATA_BACKUP_SUCCESS = "Metadata backup successfully at %s";

    // KAP Kafka
    private final String INVALID_KAFKA_DEFINITION = "The KafkaConfig definition is invalid.";

    // KAP system
    private final String DOWNLOAD_FILE_CREATE_FAIL = "Failed to create the file to download.";

    // KAP Table Ext
    private final String JOB_INSTANCE_NOT_FOUND = "Cannot find job instance.";

    public String getCLEAN_FOLDER_FAIL() {
        return CLEAN_FOLDER_FAIL;
    }

    public String getQUERY_EXCEPTION_NOT_FOUND() {
        return QUERY_EXCEPTION_NOT_FOUND;
    }

    public String getQUERY_EXCEPTION_FILE_NOT_FOUND() {
        return QUERY_EXCEPTION_FILE_NOT_FOUND;
    }

    public String getQUERY_RESULT_NOT_FOUND() {
        return QUERY_RESULT_NOT_FOUND;
    }

    public String getQUERY_RESULT_FILE_NOT_FOUND() {
        return QUERY_RESULT_FILE_NOT_FOUND;
    }

    public String getCUBE_NOT_FOUND() {
        return CUBE_NOT_FOUND;
    }

    public String getUPDATE_CUBE_NO_RIGHT() {
        return UPDATE_CUBE_NO_RIGHT;
    }

    public String getINVALID_CUBE_DEFINITION() {
        return INVALID_CUBE_DEFINITION;
    }

    public String getDISCARD_JOB_FIRST() {
        return DISCARD_JOB_FIRST;
    }

    public String getRAW_SEG_SIZE_NOT_ONE() {
        return RAW_SEG_SIZE_NOT_ONE;
    }

    public String getEMPTY_USER_NAME() {
        return EMPTY_USER_NAME;
    }

    public String getSHORT_PASSWORD() {
        return SHORT_PASSWORD;
    }

    public String getINVALID_PASSWORD() {
        return INVALID_PASSWORD;
    }

    public String getPERMISSION_DENIED() {
        return PERMISSION_DENIED;
    }

    public String getOLD_PASSWORD_WRONG() {
        return OLD_PASSWORD_WRONG;
    }

    public String getINVALID_RAWTABLE_DEFINITION() {
        return INVALID_RAWTABLE_DEFINITION;
    }

    public String getEMPTY_RAWTABLE_NAME() {
        return EMPTY_RAWTABLE_NAME;
    }

    public String getRAWTABLE_ALREADY_EXIST() {
        return RAWTABLE_ALREADY_EXIST;
    }

    public String getRAW_DESC_ALREADY_EXIST() {
        return RAW_DESC_ALREADY_EXIST;
    }

    public String getRAWTABLE_NOT_FOUND() {
        return RAWTABLE_NOT_FOUND;
    }

    public String getRAW_DESC_RENAME() {
        return RAW_DESC_RENAME;
    }

    public String getRAWTABLE_SCHEMA_CHANGE_WITH_RUNNING_JOB() {
        return RAWTABLE_SCHEMA_CHANGE_WITH_RUNNING_JOB;
    }

    public String getRAWTABLE_HAS_RUNNING_JOB() {
        return RAWTABLE_HAS_RUNNING_JOB;
    }

    public String getENABLE_NOT_DISABLED_RAWTABLE() {
        return ENABLE_NOT_DISABLED_RAWTABLE;
    }

    public String getRAWTABLE_NO_READY_SEGMENT() {
        return RAWTABLE_NO_READY_SEGMENT;
    }

    public String getRAWTABLE_ENABLE_WITH_RUNNING_JOB() {
        return RAWTABLE_ENABLE_WITH_RUNNING_JOB;
    }

    public String getDISABLE_NOT_READY_RAWTABLE() {
        return DISABLE_NOT_READY_RAWTABLE;
    }

    public String getRAWTABLE_DESC_NOT_FOUND() {
        return RAWTABLE_DESC_NOT_FOUND;
    }

    public String getUNEXPECTED_RAWTABLE_DESC_STATUS() {
        return UNEXPECTED_RAWTABLE_DESC_STATUS;
    }

    public String getNO_DRAFT_RAWTABLE_TO_UPDATE() {
        return NO_DRAFT_RAWTABLE_TO_UPDATE;
    }

    public String getNON_DRAFT_RAWTABLE_ALREADY_EXIST() {
        return NON_DRAFT_RAWTABLE_ALREADY_EXIST;
    }

    public String getRAWTABLE_RENAME() {
        return RAWTABLE_RENAME;
    }

    public String getORIGIN_RAWTABLE_NOT_FOUND() {
        return ORIGIN_RAWTABLE_NOT_FOUND;
    }

    public String getSCHEDULER_JOB_NOT_FOUND() {
        return SCHEDULER_JOB_NOT_FOUND;
    }

    public String getUNIQUE_SEQ_ID_REQUIRED() {
        return UNIQUE_SEQ_ID_REQUIRED;
    }

    public String getSTEPID_NOT_DEFAULT() {
        return STEPID_NOT_DEFAULT;
    }

    public String getSQL_AND_RESULT_OPT_REQUIRED() {
        return SQL_AND_RESULT_OPT_REQUIRED;
    }

    public String getSQL_REQUIRED() {
        return SQL_REQUIRED;
    }

    public String getEXISTING_STEPID_REQUIRED() {
        return EXISTING_STEPID_REQUIRED;
    }

    public String getUPDATE_STEP0_RESULT_OPT() {
        return UPDATE_STEP0_RESULT_OPT;
    }

    public String getSQL_OR_RESULT_OPT_REQUIRED() {
        return SQL_OR_RESULT_OPT_REQUIRED;
    }

    public String getONE_SHARED_RESULT_NULL() {
        return ONE_SHARED_RESULT_NULL;
    }

    public String getONE_SHARED_EXCEPTION() {
        return ONE_SHARED_EXCEPTION;
    }

    public String getSEQ_TOPOLOGY_NOT_FOUND() {
        return SEQ_TOPOLOGY_NOT_FOUND;
    }

    public String getTOPOLOGY_FINAL_RESULT_NOT_FOUND() {
        return TOPOLOGY_FINAL_RESULT_NOT_FOUND;
    }

    public String getQUERY_NOT_ALLOWED() {
        return QUERY_NOT_ALLOWED;
    }

    public String getNOT_SUPPORTED_SQL() {
        return NOT_SUPPORTED_SQL;
    }

    public String getEMPTY_FEATURE_NAME() {
        return EMPTY_FEATURE_NAME;
    }

    public String getDIAG_NOT_FOUND() {
        return DIAG_NOT_FOUND;
    }

    public String getGENERATE_KYBOT_PACKAGE_FAIL() {
        return GENERATE_KYBOT_PACKAGE_FAIL;
    }

    public String getKYBOT_PACKAGE_NOT_AVAILABLE() {
        return KYBOT_PACKAGE_NOT_AVAILABLE;
    }

    public String getKYBOT_PACKAGE_NOT_FOUND() {
        return KYBOT_PACKAGE_NOT_FOUND;
    }

    public String getDUMP_KYBOT_PACKAGE_FAIL() {
        return DUMP_KYBOT_PACKAGE_FAIL;
    }

    public String getUSERNAME_OR_PASSWORD_EMPTY() {
        return USERNAME_OR_PASSWORD_EMPTY;
    }

    public String getAUTH_FAIL() {
        return AUTH_FAIL;
    }

    public String getKYLIN_HOME_UNDEFINED() {
        return KYLIN_HOME_UNDEFINED;
    }

    public String getINVALID_KAFKA_DEFINITION() {
        return INVALID_KAFKA_DEFINITION;
    }

    public String getDOWNLOAD_FILE_CREATE_FAIL() {
        return DOWNLOAD_FILE_CREATE_FAIL;
    }

    public String getJOB_INSTANCE_NOT_FOUND() {
        return JOB_INSTANCE_NOT_FOUND;
    }

    public String getMETADATA_BACKUP_SUCCESS() {
        return METADATA_BACKUP_SUCCESS;
    }
}
