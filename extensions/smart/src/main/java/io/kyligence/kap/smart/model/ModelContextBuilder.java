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

package io.kyligence.kap.smart.model;

import java.util.Collection;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerFactory;

public class ModelContextBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ModelContextBuilder.class);

    private KylinConfig kylinConfig;
    private String projectName;

    public ModelContextBuilder(KylinConfig kylinConfig, String project) {
        this.kylinConfig = kylinConfig;
        this.projectName = project;
    }

    public Map<TableDesc, ModelContext> buildFromSQLs(String[] sqls) {
        final Map<TableDesc, ModelContext> emptyMap = Maps.newHashMap();

        if (sqls == null || sqls.length <= 0) {
            return emptyMap;
        }

        try (AbstractQueryRunner extractor = QueryRunnerFactory.createForModelSuggestion(kylinConfig, sqls, 1,
                projectName);) {
            extractor.execute();
            return buildFromOLAPContexts(extractor.getAllOLAPContexts());
        } catch (Exception e) {
            logger.error("Failed to execute query stats. ", e);
        }

        return emptyMap;
    }

    public Map<TableDesc, ModelContext> buildFromOLAPContexts(Map<String, Collection<OLAPContext>> allContexts) {
        Map<TableDesc, ModelContext> tableModelMap = Maps.newHashMap();
        for (Map.Entry<String, Collection<OLAPContext>> sqlContexts : allContexts.entrySet()) {
            String sql = sqlContexts.getKey();
            for (OLAPContext ctx : sqlContexts.getValue()) {
                if (ctx.firstTableScan == null) {
                    continue;
                }

                TableDesc tableDesc = ctx.firstTableScan.getTableRef().getTableDesc();
                ModelContext context = tableModelMap.get(tableDesc);
                if (context == null) {
                    context = new ModelContext(kylinConfig, projectName, tableDesc);
                    tableModelMap.put(tableDesc, context);
                }
                context.addContext(sql, ctx);
            }
        }
        return tableModelMap;
    }
}
