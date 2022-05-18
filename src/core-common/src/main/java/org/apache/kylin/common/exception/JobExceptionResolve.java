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

package org.apache.kylin.common.exception;

public enum JobExceptionResolve implements ExceptionResolveSupplier{
    JOB_BUILDING_ERROR("KE-030001000"),

    JOB_DATE_FORMAT_NOT_MATCH_ERROR("KE-030001003"),
    JOB_OUT_OF_MEMORY_ERROR("KE-030001004"),
    JOB_NO_SPACE_LEFT_ON_DEVICE_ERROR("KE-030001005"),
    JOB_CLASS_NOT_FOUND_ERROR("KE-030001006"),
    KERBEROS_REALM_NOT_FOUND("KE-030001007"),
    JOB_INT_DATE_FORMAT_NOT_MATCH_ERROR("KE-030001008"),
    UNABLE_CONNECT_SPARK_MASTER_MAXIMUM_TIME("KE-030001009");


    private final ExceptionResolve resolve;

    JobExceptionResolve(String code) {
        resolve = new ExceptionResolve(code);
    }

    @Override
    public ExceptionResolve toExceptionResolve() {
        return resolve;
    }
}
