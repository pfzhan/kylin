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
package org.apache.kylin.rest.exception;

import org.apache.kylin.common.exception.ErrorCode;
import org.apache.kylin.common.exception.ErrorCodeSupplier;

public enum ServerErrorCode implements ErrorCodeSupplier {
    // 10000XXX general
    EMPTY_ID("KE-10000001"), //
    INVALID_RANGE("KE-10000002"), //
    INVALID_PARAMETER("KE-10000003"), //
    INVALID_NAME("KE-10000004"), //
    EMPTY_PARAMETER("KE-10000005"), //

    // 10001XXX project
    PROJECT_NOT_EXIST("KE-10001001"), //
    EMPTY_PROJECT_NAME("KE-10001002"), //
    INVALID_PROJECT_NAME("KE-10001003"), //
    DUPLICATE_PROJECT_NAME("KE-10001004"), //

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

    // 10003XXX user
    USER_NOT_EXIST("KE-10003001"), //
    EMPTY_USER_NAME("KE-10003002"), //
    INVALID_USER_NAME("KE-10003003"), //
    DUPLICATE_USER_NAME("KE-10003004"), //
    USER_LOCKED("KE-10003005"), //
    FAILED_UPDATE_USER("KE-10003006"), //
    USER_UNAUTHORIZED("KE-10003007"), //

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

    // 10008XXX database
    DATABASE_NOT_EXIST("KE-10008001"), //
    FAILED_IMPORT_SSB_DATA("KE-10008002"), //
    UNSUPPORTED_DATA_SOURCE_TYPE("KE-10008003"), //

    // 10009XXX measure
    DUPLICATE_MEASURE_NAME("KE-10009001"), //
    DUPLICATE_MEASURE_EXPRESSION("KE-10009002"), //

    // 10010XXX dimension
    DUPLICATE_DIMENSION_NAME("KE-10010001"), //

    // 10011XXX cc
    DUPLICATE_COMPUTER_COLUMN_NAME("KE-10011001"), //
    DUPLICATE_COMPUTER_COLUMN_EXPRESSION("KE-10011002"), //

    // 10012XXX index
    FAILED_UPDATE_TABLE_INDEX("KE-10012001"), //
    FAILED_UPDATE_AGG_INDEX("KE-10012002"), //
    NO_INDEX_DEFINE("KE-10012003"), //

    // 10013XXX job
    FAILED_UPDATE_JOB_STATUS("KE-10013001"), //
    ILLEGAL_JOB_STATUS("KE-10013002"), //
    EMPTY_JOB_ID("KE-10013003"), //
    INVALID_SAMPLING_RANGE("KE-10013004"), //

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

    // 10018XXX kerberos
    INVALID_KERBEROS_FILE("KE-10018001"), //
    FAILED_CHECK_KERBEROS("KE-10018002"), //

    // 10019XXX catalog
    FAILED_REFRESH_CATALOG_CACHE("KE-10019001"), //

    // 10020XXX recommendation
    FAILED_APPROVE_RECOMMENDATION("KE-10020001"), //
    UNSUPPORTED_RECOMMENDATION_MODE("KE-10020002"), //

    // 10021XXX server
    REMOTE_SERVER_ERROR("KE-10021001"), //
    SYSTEM_IS_RECOVER("KE-10021002"), //
    TRANSFER_FAILED("KE-10021003"), //
    NO_ACTIVE_ALL_NODE("KE-10021004"), //

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

    // 10024XXX auth
    ACCESS_DENIED("KE-10024001"), //
    PERMISSION_DENIED("KE-10024002"), //

    // 10027XXX frequency query
    EMPTY_FREQUENCY_RULE_VALUE("KE-10027001"), //
    EMPTY_DURATION_RULE_VALUE("KE-10027002"), //

    // 10028XXX json
    FAILED_PARSE_JSON("KE-10028001");//

    private final ErrorCode errorCode;

    ServerErrorCode(String code) {
        errorCode = new ErrorCode(code);
    }

    @Override
    public ErrorCode toErrorCode() {
        return errorCode;
    }
}
