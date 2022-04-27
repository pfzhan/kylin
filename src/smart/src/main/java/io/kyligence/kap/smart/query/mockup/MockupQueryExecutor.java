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

import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.debug.BackdoorToggles;
import org.apache.kylin.common.util.ThreadUtil;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.query.util.QueryParams;
import org.apache.kylin.query.util.QueryUtil;

import io.kyligence.kap.guava20.shaded.common.base.Stopwatch;
import io.kyligence.kap.query.engine.QueryExec;
import io.kyligence.kap.query.util.KapQueryUtil;
import io.kyligence.kap.smart.query.QueryRecord;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MockupQueryExecutor extends AbstractQueryExecutor {

    private final boolean queryNonEquiJoinEnabled;

    public MockupQueryExecutor() {
        this.queryNonEquiJoinEnabled = BackdoorToggles.getIsQueryNonEquiJoinModelEnabled();
    }

    public QueryRecord execute(String project, KylinConfig kylinConfig, String sql) {
        Stopwatch watch = Stopwatch.createStarted();
        OLAPContext.clearThreadLocalContexts();
        //set to check all models, rather than skip models when finding a realization in RealizationChooser#attemptSelectRealization
        BackdoorToggles.addToggle(BackdoorToggles.DEBUG_TOGGLE_CHECK_ALL_MODELS, "true");
        BackdoorToggles.addToggle(BackdoorToggles.DISABLE_RAW_QUERY_HACKER, "true");
        BackdoorToggles.addToggle(BackdoorToggles.QUERY_FROM_AUTO_MODELING, "true");
        BackdoorToggles.addToggle(BackdoorToggles.DEBUG_TOGGLE_PREPARE_ONLY, "true");

        if (queryNonEquiJoinEnabled) {
            BackdoorToggles.addToggle(BackdoorToggles.QUERY_NON_EQUI_JOIN_MODEL_ENABLED, "true");
        }
        QueryRecord record = getCurrentRecord();
        if (!QueryUtil.isSelectStatement(sql)) {
            record.noteNonQueryException(project, sql, watch.elapsed(TimeUnit.MILLISECONDS));
            return record;
        }

        try {
            // execute and discard the result data
            QueryExec queryExec = new QueryExec(project, kylinConfig);
            QueryParams queryParams = new QueryParams(kylinConfig, sql, project, 0, 0, queryExec.getDefaultSchemaName(),
                    true);
            queryExec.executeQuery(KapQueryUtil.massageSql(queryParams));
        } catch (Throwable e) { // cannot replace with Exception, e may a instance of Error
            Throwable cause = e.getCause();
            String message = e.getMessage() == null
                    ? String.format(Locale.ROOT, "%s, check kylin.log for details", e.getClass().toString())
                    : QueryUtil.makeErrorMsgUserFriendly(e);
            if (!(cause instanceof NoRealizationFoundException)) {
                log.debug("Failed to run in MockupQueryExecutor. Critical stackTrace:\n{}",
                        ThreadUtil.getKylinStackTrace());
            }
            record.noteException(message, e);
        } finally {
            record.noteNormal(project, sql, watch.elapsed(TimeUnit.MILLISECONDS), QueryContext.current().getQueryId());
            record.noteOlapContexts(kylinConfig);
            if (CollectionUtils.isNotEmpty(record.getOlapContexts())) {
                record.getOlapContexts().forEach(ctx -> ctx.aggregations = transformSpecialFunctions(ctx));
            }
            clearCurrentRecord();
        }

        return record;
    }

    private List<FunctionDesc> transformSpecialFunctions(OLAPContext ctx) {
        return ctx.aggregations.stream().map(func -> {
            if (FunctionDesc.FUNC_INTERSECT_COUNT.equalsIgnoreCase(func.getExpression())) {
                ctx.getGroupByColumns().add(func.getParameters().get(1).getColRef());
                return FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT_DISTINCT, func.getParameters().subList(0, 1),
                        "bitmap");
            } else if (FunctionDesc.FUNC_BITMAP_UUID.equalsIgnoreCase((func.getExpression()))) {
                return FunctionDesc.newInstance(FunctionDesc.FUNC_COUNT_DISTINCT, func.getParameters().subList(0, 1),
                        "bitmap");
            } else {
                return func;
            }
        }).collect(Collectors.toList());
    }
}
