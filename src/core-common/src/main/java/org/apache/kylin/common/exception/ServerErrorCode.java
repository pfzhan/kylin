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
package org.apache.kylin.common.exception;

public enum ServerErrorCode implements ErrorCodeSupplier {
    // 10000XXX general
    EMPTY_ID("KE-10000001"), //
    INVALID_RANGE("KE-10000002"), //
    INVALID_PARAMETER("KE-10000003"), //
    INVALID_NAME("KE-10000004"), //
    EMPTY_PARAMETER("KE-10000005"), //
    UNAUTHORIZED_ENTITY("KE-10000006"), //
    FAILED_DETECT_DATA_RANGE("KE-10000007"), //
    ONGOING_OPTIMIZATION("KE-10000008"), //

    // 10001XXX project
    PROJECT_NOT_EXIST("KE-10001001"), //
    EMPTY_PROJECT_NAME("KE-10001002"), //
    INVALID_PROJECT_NAME("KE-10001003"), //
    DUPLICATE_PROJECT_NAME("KE-10001004"), //
    PROJECT_NAME_ILLEGAL("KE-10001005"), //
    FAILED_CREATE_PROJECT("KE-10001006"), //
    INCORRECT_PROJECT_MODE("KE-10001007"), //

    // 10002XXX model
    MODEL_NOT_EXIST("KE-10002001"), //
    EMPTY_MODEL_NAME("KE-10002002"), //
    INVALID_MODEL_NAME("KE-10002003"), //
    DUPLICATE_MODEL_NAME("KE-10002004"), //
    FAILED_UPDATE_MODEL("KE-10002005"), //
    FAILED_IMPORT_MODEL("KE-10002006"), //
    MODEL_UNSUPPORTED_OPERATOR("KE-10002007"), //
    EMPTY_MODEL_ID("KE-10002008"), //
    MODEL_METADATA_FILE_ERROR("KE-10002009"), //
    FAILED_CREATE_MODEL("KE-10002010"), //
    MODEL_BROKEN("KE-10002011"), //
    MODEL_ONLINE_ABANDON("KE-10002012"), //
    INVALID_MEASURE_DATA_TYPE("KE-10002013"), //
    MODEL_METADATA_CONFLICT_ERROR("KE-10002014"), //
    MODEL_EXPORT_ERROR("KE-10002015"), //
    MODEL_IMPORT_ERROR("KE-10002016"), //
    INVALID_MODEL_TYPE("KE-10002017"), //
    PARTITION_VALUE_NOT_SUPPORT("KE-10002018"),

    // 10003XXX user
    USER_NOT_EXIST("KE-10003001"), //
    EMPTY_USER_NAME("KE-10003002"), //
    INVALID_USER_NAME("KE-10003003"), //
    DUPLICATE_USER_NAME("KE-10003004"), //
    USER_LOCKED("KE-10003005"), //
    FAILED_UPDATE_USER("KE-10003006"), //
    USER_UNAUTHORIZED("KE-10003007"), //
    LOGIN_FAILED("KE-10003008"), //

    // 10004XXX user group
    USERGROUP_NOT_EXIST("KE-10004001"), //
    EMPTY_USERGROUP_NAME("KE-10004002"), //
    INVALID_USERGROUP_NAME("KE-10004003"), //
    DUPLICATE_USERGROUP_NAME("KE-10004004"), //

    // 10005XXX password
    INVALID_PASSWORD("KE-10005001"), //
    SHORT_PASSWORD("KE-10005002"), //
    FAILED_UPDATE_PASSWORD("KE-10005003"), //

    // 10006XXX column
    PARTITION_COLUMN_NOT_EXIST("KE-10006001"), //
    INVALID_PARTITION_COLUMN("KE-10006002"), //
    COLUMN_NOT_EXIST("KE-10006003"), //
    EMPTY_PARTITION_COLUMN("KE-10006004"), //

    // 10007XXX table
    TABLE_NOT_EXIST("KE-10007001"), //
    EMPTY_TABLE_NAME("KE-10007002"), //
    INVALID_TABLE_NAME("KE-10007003"), //
    RELOAD_TABLE_FAILED("KE-10007004"), //
    INVALID_TABLE_REFRESH_PARAMETER("KE-10007005"), //
    INVALID_TABLE_SAMPLE_RANGE("KE-10007006"), //
    DUPLICATED_COLUMN_NAME("KE-10007007"), //
    ON_GOING_JOB_EXIST("KE-10007008"), //

    // 10008XXX database
    DATABASE_NOT_EXIST("KE-10008001"), //
    FAILED_IMPORT_SSB_DATA("KE-10008002"), //
    UNSUPPORTED_DATA_SOURCE_TYPE("KE-10008003"), //

    // 10009XXX measure
    DUPLICATE_MEASURE_NAME("KE-10009001"), //
    DUPLICATE_MEASURE_EXPRESSION("KE-10009002"), //

    // 10010XXX dimension
    DUPLICATE_DIMENSION_NAME("KE-10010001"), //
    EFFECTIVE_DIMENSION_NOT_FIND("KE-10010002"),

    // 10011XXX cc
    DUPLICATE_COMPUTED_COLUMN_NAME("KE-10011001"), //
    DUPLICATE_COMPUTED_COLUMN_EXPRESSION("KE-10011002"), //
    INVALID_COMPUTED_COLUMN_EXPRESSION("KE-10011003"), //
    COMPUTED_COLUMN_CASCADE_ERROR("KE-10011004"), //

    // 10012XXX index
    FAILED_UPDATE_TABLE_INDEX("KE-10012001"), //
    FAILED_UPDATE_AGG_INDEX("KE-10012002"), //
    NO_INDEX_DEFINE("KE-10012003"), //
    RULE_BASED_INDEX_METADATA_INCONSISTENT("KE-10012004"), //

    // 10013XXX job
    FAILED_UPDATE_JOB_STATUS("KE-10013001"), //
    ILLEGAL_JOB_STATUS("KE-10013002"), //
    EMPTY_JOB_ID("KE-10013003"), //
    INVALID_SAMPLING_RANGE("KE-10013004"), //
    ILLEGAL_JOB_STATE_TRANSFER("KE-10013005"), //
    CONCURRENT_SUBMIT_JOB_LIMIT("KE-10013006"), //

    // 10014XXX sql expression
    INVALID_FILTER_CONDITION("KE-10014001"), //
    FAILED_EXECUTE_MODEL_SQL("KE-10014002"), //
    DUPLICATE_JOIN_CONDITION("KE-10014003"), //
    EMPTY_SQL_EXPRESSION("KE-10014004"), //

    // 10015XXX license
    LICENSE_FILE_NOT_EXIST("KE-10015001"), //
    EMPTY_LICENSE_CONTENT("KE-10015002"), //
    INVALID_LICENSE("KE-10015003"), //

    // 10016XXX email
    EMPTY_EMAIL("KE-10016001"), //
    INVALID_EMAIL("KE-10016002"), //

    // 10017XXX file
    FILE_NOT_EXIST("KE-10017001"), //
    FAILED_DOWNLOAD_FILE("KE-10017002"), //
    FILE_FORMAT_ERROR("KE-10017003"), //
    EMPTY_FILE_CONTENT("KE-10017004"), //
    SQL_NUMBER_EXCEEDS_LIMIT("KE-10017005"), //
    FILE_TYPE_MISMATCH("KE-10017006"),

    // 10018XXX kerberos
    INVALID_KERBEROS_FILE("KE-10018001"), //
    FAILED_CHECK_KERBEROS("KE-10018002"), //

    // 10019XXX catalog
    FAILED_REFRESH_CATALOG_CACHE("KE-10019001"), //
    FAILED_CONNECT_CATALOG("KE-10019002"),

    // 10020XXX recommendation
    FAILED_APPROVE_RECOMMENDATION("KE-10020001"), //
    REC_LIST_OUT_OF_DATE("KE-10020002"), //
    UNSUPPORTED_REC_OPERATION_TYPE("KE-10020003"), //

    // 10021XXX server
    REMOTE_SERVER_ERROR("KE-10021001"), //
    SYSTEM_IS_RECOVER("KE-10021002"), //
    TRANSFER_FAILED("KE-10021003"), //
    NO_ACTIVE_ALL_NODE("KE-10021004"), //
    PROJECT_WITHOUT_RESOURCE_GROUP("KE-10021005"),

    // 10022XXX segment
    INVALID_SEGMENT_RANGE("KE-10022001"), //
    SEGMENT_RANGE_OVERLAP("KE-10022002"), //
    FAILED_REFRESH_SEGMENT("KE-10022003"), //
    SEGMENT_UNSUPPORTED_OPERATOR("KE-10022004"), //
    EMPTY_SEGMENT_ID("KE-10022005"), //
    FAILED_MERGE_SEGMENT("KE-10022006"), //
    EMPTY_SEGMENT_RANGE("KE-10022007"), //
    SEGMENT_NOT_EXIST("KE-10022008"), //
    INVALID_SEGMENT_PARAMETER("KE-10022009"), //

    // 10023XXX diag
    DIAG_UUID_NOT_EXIST("KE-10023001"), //
    DIAG_PACKAGE_NOT_READY("KE-10023002"), //
    DIAG_FAILED("KE-10023003"), //

    // 10024XXX auth
    ACCESS_DENIED("KE-10024001"), //
    PERMISSION_DENIED("KE-10024002"), //

    // 10027XXX frequency query
    EMPTY_FREQUENCY_RULE_VALUE("KE-10027001"), //
    EMPTY_DURATION_RULE_VALUE("KE-10027002"), //
    EMPTY_COUNT_RULE_VALUE("KE-10027003"), //
    FAILED_ACCELERATE_QUERY("KE-10027004"), //
    FAILED_INSERT_ACCELERATE_QUERY_BLACKLIST("KE-10027005"), //
    EMPTY_REC_RULE_VALUE("KE-10027003"), //

    // 10031XXX query result
    FAILED_OBTAIN_QUERY_RESULT("KE-10031001"),

    // 10032xxx add job result
    FAILED_CREATE_JOB("KE-10032001"),

    // 10033xx snapshot
    SNAPSHOT_NOT_EXIST("KE-10033001"), //
    SNAPSHOT_MANAGEMENT_NOT_ENABLED("KE-10033002"),

    // 10034xx acl
    ACL_DEPENDENT_COLUMN_PARSE_ERROR("KE-10034001"),

    // 10034xxx multi partition column
    INVALID_MULTI_PARTITION_MAPPING_REQUEST("KE-10035001");

    private final ErrorCode errorCode;

    ServerErrorCode(String code) {
        errorCode = new ErrorCode(code);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
