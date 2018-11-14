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

package io.kyligence.kap.smart.query.mockup;

import org.apache.kylin.query.enumerator.LookupTableEnumerator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.smart.query.QueryRecord;
import io.kyligence.kap.smart.query.SQLResult;

public abstract class AbstractQueryExecutor {

    static final Logger logger = LoggerFactory.getLogger(AbstractQueryExecutor.class);

    private static final ThreadLocal<QueryRecord> CURRENT_RECORD = new ThreadLocal<>();

    static QueryRecord getCurrentRecord() {
        QueryRecord record = CURRENT_RECORD.get();
        if (record == null) {
            record = new QueryRecord();
            CURRENT_RECORD.set(record);
        }
        return record;
    }

    static void clearCurrentRecord() {
        CURRENT_RECORD.remove();
    }

    /**
     * Execute the given SQL statement under a certain project name,
     * which returns a <code>QueryRecord</code> object.
     *
     * @param projectName project name
     * @param sql any SQL statement, but only <code>SELECT</code> statement
     *            gives what you want.
     * @return a <code>QueryRecord</code> object that contains the data
     * produced by the given query; never null
     */
    public abstract QueryRecord execute(String projectName, String sql);

    /**
     * if stack trace elements contains LookupTableEnumerator, update the status of SQLResult and return true
     */
    boolean isSqlResultStatusModifiedByExceptionCause(SQLResult sqlResult, StackTraceElement[] stackTraceElements) {
        for (StackTraceElement element : stackTraceElements) {
            if (element.toString().contains(LookupTableEnumerator.class.getName())) {
                logger.debug("This query hits table snapshot.");

                sqlResult.setStatus(SQLResult.Status.SUCCESS);
                return true;
            }
        }
        return false;
    }
}
