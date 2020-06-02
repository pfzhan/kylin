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

import java.sql.SQLException;
import java.util.Collection;

import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;

import com.google.common.cache.CacheLoader.InvalidCacheLoadException;

import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.smart.query.QueryRecord;
import io.kyligence.kap.smart.query.SQLResult;

public class MockupQueryExecutor extends AbstractQueryExecutor {

    public QueryRecord execute(String projectName, String sql) {
        OLAPContext.clearThreadLocalContexts();
        //set to check all models, rather than skip models when finding a realization in RealizationChooser#attemptSelectRealization
        BackdoorToggles.addToggle(BackdoorToggles.DEBUG_TOGGLE_CHECK_ALL_MODELS, "true");
        BackdoorToggles.addToggle(BackdoorToggles.DISABLE_RAW_QUERY_HACKER, "true");
        BackdoorToggles.addToggle(BackdoorToggles.QUERY_FROM_AUTO_MODELING, "true");
        BackdoorToggles.addToggle(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");

        QueryRecord record = getCurrentRecord();
        SQLResult sqlResult = new SQLResult();
        record.setSqlResult(sqlResult);

        if (!QueryUtil.isSelectStatement(sql)) {
            sqlResult.setStatus(SQLResult.Status.FAILED);
            sqlResult.setMessage("Not Supported SQL.");
            sqlResult.setException(new SQLException("Not Supported SQL."));
            return record;
        }

        try {
            // execute and discard the result data
            KylinConfig projectKylinConfig = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                    .getProject(projectName).getConfig();
            QueryExec queryExec = new QueryExec(projectName, projectKylinConfig);
            QueryParams queryParams = new QueryParams(QueryUtil.getKylinConfig(projectName), sql, projectName, 0, 0,
                    queryExec.getSchema(), true);
            queryExec.executeQuery(QueryUtil.massageSql(queryParams));

            sqlResult.setStatus(SQLResult.Status.SUCCESS);
        } catch (Throwable e) { // cannot replace with Exception, e may a instance of Error
            Throwable cause = e.getCause();
            boolean printException = true;
            if (cause instanceof InvalidCacheLoadException) {
                if (isSqlResultStatusModifiedByExceptionCause(sqlResult, cause.getStackTrace())) {
                    return record;
                }
            } else if (cause instanceof NoRealizationFoundException) {
                printException = false;
            }

            String message = e.getMessage() == null
                    ? String.format("%s, check kylin.log for details", e.getClass().toString())
                    : QueryUtil.makeErrorMsgUserFriendly(e);
            if (printException) {
                logger.debug("Failed to run in MockupQueryExecutor.", e);
            }

            sqlResult.setStatus(SQLResult.Status.FAILED);
            sqlResult.setMessage(message);
            sqlResult.setException(e);
        } finally {
            Collection<OLAPContext> ctxs = OLAPContext.getThreadLocalContexts();
            if (ctxs != null) {
                ctxs.stream().forEach(context -> context.clean());
            }
            record.setOLAPContexts(ctxs);
            clearCurrentRecord();
        }

        return record;
    }
}
