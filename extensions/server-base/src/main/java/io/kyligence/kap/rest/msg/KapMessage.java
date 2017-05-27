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

    // KAP Async Query
    public String getCLEAN_FOLDER_FAIL() {
        return "Failed to clean folder.";
    }

    public String getQUERY_EXCEPTION_NOT_FOUND() {
        return "No exception for this query is available, please check its status first.";
    }

    public String getQUERY_EXCEPTION_FILE_NOT_FOUND() {
        return "The query exception file does not exist.";
    }

    public String getQUERY_RESULT_NOT_FOUND() {
        return "No result for this query is available, please check its status first.";
    }

    public String getQUERY_RESULT_FILE_NOT_FOUND() {
        return "The query result file does not exist.";
    }

    // Cube
    public String getCUBE_NOT_FOUND() {
        return "Cannot find cube '%s'.";
    }

    public String getUPDATE_CUBE_NO_RIGHT() {
        return "You don't have right to update this cube.";
    }

    public String getINVALID_CUBE_DEFINITION() {
        return "The cube definition is invalid.";
    }

    // KAP Cube
    public String getRAW_SEG_SIZE_NOT_ONE() {
        return "There should be exactly one rawtable segment.";
    }

    // KAP User
    public String getEMPTY_USER_NAME() {
        return "User name should not be empty.";
    }

    public String getSHORT_PASSWORD() {
        return "The password should contain more than 8 characters!";
    }

    public String getINVALID_PASSWORD() {
        return "The password should contain at least one numbers, letters and special characters (~!@#$%^&*(){}|:\"<>?[];\\'\\,./`).";
    }

    public String getPERMISSION_DENIED() {
        return "Permission denied!";
    }

    public String getOLD_PASSWORD_WRONG() {
        return "Old password is not correct!";
    }

    // KAP Raw Table
    public String getINVALID_RAWTABLE_DEFINITION() {
        return "The rawTable definition is not valid.";
    }

    public String getEMPTY_RAWTABLE_NAME() {
        return "RawTable name should not be empty.";
    }

    public String getRAWTABLE_ALREADY_EXIST() {
        return "The rawTable named '%s' already exists.";
    }

    public String getRAWTABLE_NOT_FOUND() {
        return "The rawTable named '%s' does not exist.";
    }

    public String getRAW_DESC_RENAME() {
        return "Raw Desc renaming is not allowed: new: '%s', origin: '%s'.";
    }

    public String getRAWTABLE_SCHEMA_CHANGE_WITH_RUNNING_JOB() {
        return "RawTable schema shouldn't be changed with running job.";
    }

    public String getRAWTABLE_HAS_RUNNING_JOB() {
        return "The rawTable '%s' has running job, please discard it and try again.";
    }

    public String getENABLE_NOT_DISABLED_RAWTABLE() {
        return "Only disabled rawTable can be enabled, status of '%s' is %s.";
    }

    public String getRAWTABLE_NO_READY_SEGMENT() {
        return "RawTable '%s' doesn't contain any READY segment.";
    }

    public String getRAWTABLE_ENABLE_WITH_RUNNING_JOB() {
        return "Enable is not allowed with a running job.";
    }

    public String getDISABLE_NOT_READY_RAWTABLE() {
        return "Only ready rawTable can be disabled, status of '%s' is %s.";
    }

    public String getRAWTABLE_DESC_NOT_FOUND() {
        return "The rawTable desc named '%s' does not exist.";
    }

    public String getRAWTABLE_RENAME() {
        return "RawTable renaming is not allowed.";
    }

    // KAP Sequence SQL
    public String getUNIQUE_SEQ_ID_REQUIRED() {
        return "Must provided a unique sequenceID for a SQL sequence.";
    }

    public String getSTEPID_NOT_DEFAULT() {
        return "If you're not updating a certain sql, you should leave stepID as default (-1).";
    }

    public String getSQL_AND_RESULT_OPT_REQUIRED() {
        return "Sql and result opt are required.";
    }

    public String getSQL_REQUIRED() {
        return "Sql is required.";
    }

    public String getEXISTING_STEPID_REQUIRED() {
        return "If you're updating a certain sql, you should provide an existing stepID.";
    }

    public String getUPDATE_STEP0_RESULT_OPT() {
        return "Result opt cannot be updated for step 0.";
    }

    public String getSQL_OR_RESULT_OPT_REQUIRED() {
        return "sql or result opt are required.";
    }

    public String getONE_SHARED_RESULT_NULL() {
        return "One of the shard result is null.";
    }

    public String getONE_SHARED_EXCEPTION() {
        return "One of the shard met exception: '%s'.";
    }

    public String getSEQ_TOPOLOGY_NOT_FOUND() {
        return "The sequence topology is not found, maybe expired?";
    }

    public String getTOPOLOGY_FINAL_RESULT_NOT_FOUND() {
        return "The final result for current topology is not found!";
    }

    // Query
    public String getQUERY_NOT_ALLOWED() {
        return "Query is not allowed in '%s' mode.";
    }

    public String getNOT_SUPPORTED_SQL() {
        return "Not Supported SQL.";
    }

    // KAP Config
    public String getEMPTY_FEATURE_NAME() {
        return "Request field feature_name cannot be empty.";
    }

    // KAP KyBot
    public String getDUMP_KYBOT_PACKAGE_FAIL() {
        return "Failed to dump kybot package.";
    }

    // KAP Metastore
    public String getKYLIN_HOME_UNDEFINED() {
        return "KYLIN_HOME undefined.";
    }

    // streaming
    public String getINVALID_KAFKA_CONFIG_DEFINITION() {
        return "The KafkaConfig definition is invalid.";
    }

    // KAP system
    public String getDOWNLOAD_FILE_CREATE_FAIL() {
        return "Failed to create the file to download.";
    }

    // KAP Table Ext
    public String getJOB_INSTANCE_NOT_FOUND() {
        return "Cannot find job instance.";
    }

    // KAP Authentication
    public String getUSER_LOCK() {
        return "User %s is locked, please wait for %s seconds.";
    }

}
