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

package io.kyligence.kap.smart.query;

import java.util.Collection;
import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.relnode.OLAPContext;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.smart.common.SmartConfig;
import lombok.Getter;

@Getter
public class QueryRecord {

    private final SQLResult sqlResult = new SQLResult();
    private final List<OLAPContext> olapContexts = Lists.newArrayList();

    public void noteNonQueryException(String project, String sql, long elapsed) {
        sqlResult.writeNonQueryException(project, sql, elapsed);
    }

    public void noteException(String message, Throwable e) {
        sqlResult.writeExceptionInfo(message, e);
    }

    public void noteNormal(String project, String sql, long elapsed, String queryId) {
        sqlResult.writeNormalInfo(project, sql, elapsed, queryId);
    }

    public void noteOlapContexts(KylinConfig config) {
        refineOlapContext(config, sqlResult.getSql());
        Collection<OLAPContext> localContexts = OLAPContext.getThreadLocalContexts();
        if (CollectionUtils.isNotEmpty(localContexts)) {
            olapContexts.addAll(localContexts);
        }
    }

    private void refineOlapContext(KylinConfig kylinConfig, String sql) {
        Collection<OLAPContext> ctxList = OLAPContext.getThreadLocalContexts();
        if (ctxList != null) {
            ctxList.forEach(OLAPContext::clean);
            ctxList.forEach(ctx -> ctx.sql = sql);
            if (SmartConfig.wrap(kylinConfig).startMemoryTuning()) {
                ctxList.forEach(OLAPContext::simplify);
            }
        }
    }
}
