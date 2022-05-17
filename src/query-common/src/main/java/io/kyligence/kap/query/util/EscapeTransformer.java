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

package io.kyligence.kap.query.util;

import org.apache.kylin.common.KapConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EscapeTransformer implements KapQueryUtil.IQueryTransformer {

    private static final Logger logger = LoggerFactory.getLogger(EscapeTransformer.class);

    private EscapeDialect dialect = EscapeDialect.DEFAULT;

    @Override
    public String transform(String sql, String project, String defaultSchema) {
        if (!KapConfig.getInstanceFromEnv().isJdbcEscapeEnabled()) {
            return sql;
        }

        return transform(sql);
    }

    public String transform(String sql) {
        try {
            // remove the comment
            sql = new RawSqlParser(sql).parse().getStatementString();
        }catch (Throwable ex) {
            logger.error("Something unexpected while CommentParser transforming the query, return original query",
                    ex);
            logger.error(sql);
        }
        try{
            EscapeParser parser = new EscapeParser(dialect, sql);
            String result = parser.Input();
            logger.debug("EscapeParser done parsing");
            return result;
        } catch (Throwable ex) {
            logger.error("Something unexpected while EscapeTransformer transforming the query, return original query",
                    ex);
            logger.error(sql);
            return sql;
        }
    }

    public void setFunctionDialect(EscapeDialect newDialect) {
        this.dialect = newDialect;
    }
}
