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

package io.kyligence.kap.smart.cube;

import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.smart.common.SmartConfig;
import io.kyligence.kap.smart.cube.domain.DefaultDomainBuilder;
import io.kyligence.kap.smart.cube.domain.Domain;
import io.kyligence.kap.smart.cube.stats.ICubeStats;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerFactory;
import io.kyligence.kap.smart.query.QueryStats;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.source.hive.modelstats.ModelStats;
import io.kyligence.kap.source.hive.modelstats.ModelStatsManager;

public class CubeContextBuilder {
    private static final Logger logger = LoggerFactory.getLogger(CubeContextBuilder.class);

    private final KylinConfig kylinConfig;
    private final SmartConfig smartConfig;
    private final TableMetadataManager tableManager;
    private final ModelStatsManager modelStatsManager;
    private final DataModelManager modelManager;

    public CubeContextBuilder(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.smartConfig = SmartConfig.wrap(kylinConfig);

        this.tableManager = TableMetadataManager.getInstance(kylinConfig);
        this.modelStatsManager = ModelStatsManager.getInstance(kylinConfig);
        this.modelManager = DataModelManager.getInstance(kylinConfig);
    }

    public Map<String, CubeContext> buildFromProject(String project, String[] sqls) {
        return internalBuildContexts(project, null, sqls);
    }

    public CubeContext buildFromModelDesc(DataModelDesc modelDesc, String[] sqls) {
        Map<String, CubeContext> result = internalBuildContexts(modelDesc.getProject(), modelDesc.getName(), sqls);
        return result.get(modelDesc.getName());
    }

    public CubeContext buildFromModelDesc(DataModelDesc modelDesc, QueryStats queryStats, List<SQLResult> sqlResults) {
        return internalBuild(modelDesc, queryStats, sqlResults);
    }

    public CubeContext buildFromCubeDesc(CubeDesc cubeDesc, String[] sqls) {
        return buildFromModelDesc(cubeDesc.getModel(), sqls);
    }

    public CubeContext buildFromCubeDesc(CubeDesc cubeDesc, QueryStats queryStats, List<SQLResult> sqlResults) {
        return buildFromModelDesc(cubeDesc.getModel(), queryStats, sqlResults);
    }

    public CubeContext buildFromCubeDesc(CubeDesc cubeDesc, ICubeStats cubeStats, String[] sqls) {
        CubeContext context = buildFromCubeDesc(cubeDesc, sqls);
        if (cubeStats != null) {
            context.setCubeStats(cubeDesc, cubeStats);
        }
        return context;
    }

    public CubeContext buildFromCubeDesc(CubeDesc cubeDesc, ICubeStats cubeStats, QueryStats queryStats,
            List<SQLResult> sqlResults) {
        CubeContext context = buildFromCubeDesc(cubeDesc, queryStats, sqlResults);
        if (cubeStats != null) {
            context.setCubeStats(cubeDesc, cubeStats);
        }
        return context;
    }

    private Map<String, CubeContext> internalBuildContexts(String project, String model, String[] sqls) {
        Map<String, QueryStats> modelQueryStats = Maps.newHashMap();
        List<SQLResult> queryResults = null;
        if (!ArrayUtils.isEmpty(sqls)) {
            try (AbstractQueryRunner extractor = QueryRunnerFactory.createForCubeSuggestion(kylinConfig, sqls,
                    smartConfig.getQueryDryRunThreads(), project)) {
                extractor.execute();
                queryResults = extractor.getQueryResults();
                List<Collection<OLAPContext>> olapContexts = extractor.getAllOLAPContexts();
                for (int i = 0; i < sqls.length; i++) {
                    Collection<OLAPContext> ctxs = olapContexts.get(i);
                    String sql = sqls[i];
                    for (OLAPContext ctx : ctxs) {
                        if (ctx.realizationCheck == null || MapUtils.isEmpty(ctx.realizationCheck.getCapableModels())) {
                            continue;
                        }

                        Map<DataModelDesc, Map<String, String>> modelMap = ctx.realizationCheck.getCapableModels();
                        for (Map.Entry<DataModelDesc, Map<String, String>> entry : modelMap.entrySet()) {
                            DataModelDesc m = entry.getKey();
                            Map<String, String> aliasMatch = entry.getValue();
                            if (model == null || m.getName().equals(model)) {
                                QueryStats s = modelQueryStats.get(m.getName());
                                if (s == null) {
                                    s = new QueryStats();
                                    modelQueryStats.put(m.getName(), s);
                                }
                                QueryStats qa = QueryStats.buildFromOLAPContext(ctx, m, aliasMatch);
                                for (String col : qa.getAppears().keySet()) {
                                    m.findColumn(col);
                                }
                                s.merge(qa);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to execute query stats. ", e);
            }
        } else {
            List<DataModelDesc> models = modelManager.getModels(project);
            for (DataModelDesc m : models) {
                if (m == null || m.getName().equals(model)) {
                    modelQueryStats.put(m.getName(), null);
                }
            }
        }
        return internalBuildContexts(project, modelQueryStats, queryResults);
    }

    private Map<String, CubeContext> internalBuildContexts(String project, Map<String, QueryStats> modelQueryStats,
            List<SQLResult> queryResults) {
        Map<String, CubeContext> result = Maps.newHashMapWithExpectedSize(modelQueryStats.size());
        List<DataModelDesc> models = modelManager.getModels(project);
        for (DataModelDesc model : models) {
            QueryStats qs = modelQueryStats.get(model.getName());
            CubeContext ctx = internalBuild(model, qs, queryResults);
            result.put(model.getName(), ctx);
        }
        return result;
    }

    private CubeContext internalBuild(DataModelDesc model, QueryStats queryStats, List<SQLResult> sqlResults) {
        CubeContext context = new CubeContext(kylinConfig);
        Domain usedDomain = new DefaultDomainBuilder(smartConfig, queryStats, model).build();

        // set model stats
        ModelStats modelStats = modelStatsManager.getModelStatsQuietly(model);
        if (modelStats != null && modelStats.getSingleColumnCardinality().isEmpty()) {
            // An empty modelStats will return from ModelStatsManager.getModelStats() if not existed.
            modelStats = null;
        }

        // set table stats
        Map<String, TableDesc> tableDescMap = Maps.newHashMap();
        Map<String, TableExtDesc> tableExtDescMap = Maps.newHashMap();
        for (TableRef tableRef : model.getAllTables()) {
            String tblRefId = tableRef.getTableIdentity();
            TableDesc tableDesc = tableRef.getTableDesc();
            tableDescMap.put(tblRefId, tableDesc);

            TableExtDesc tableExtDesc = tableManager.getTableExt(tableDesc);
            if (tableExtDesc != null && !tableExtDesc.getColumnStats().isEmpty()) {
                tableExtDescMap.put(tblRefId, tableExtDesc);
            }
        }

        context.setDomain(usedDomain);
        context.setQueryStats(queryStats);
        context.setModelStats(modelStats);
        context.setModelDesc(model);
        context.setTableDescs(tableDescMap);
        context.setTableExtDescs(tableExtDescMap);
        context.setSqlResults(sqlResults);

        return context;
    }
}
