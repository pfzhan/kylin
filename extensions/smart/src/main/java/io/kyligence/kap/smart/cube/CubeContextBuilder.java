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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;

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
import io.kyligence.kap.smart.cube.domain.ModelDomainBuilder;
import io.kyligence.kap.smart.cube.stats.ICubeStats;
import io.kyligence.kap.smart.query.AbstractQueryRunner;
import io.kyligence.kap.smart.query.QueryRunnerFactory;
import io.kyligence.kap.smart.query.QueryStats;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.advisor.ISQLAdvisor;
import io.kyligence.kap.smart.query.advisor.ModelBasedSQLAdvisor;
import io.kyligence.kap.smart.query.advisor.SQLAdvice;
import io.kyligence.kap.source.hive.modelstats.ModelStats;
import io.kyligence.kap.source.hive.modelstats.ModelStatsManager;

public class CubeContextBuilder {
    private static final Logger logger = LoggerFactory.getLogger(CubeContextBuilder.class);
    private KylinConfig kylinConfig;
    private SmartConfig smartConfig;

    public CubeContextBuilder(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
        this.smartConfig = SmartConfig.wrap(kylinConfig);
    }

    public CubeContext buildFromModelDesc(DataModelDesc modelDesc, String[] sqls) {
        Domain modelDomain = new ModelDomainBuilder(modelDesc).build();
        CubeDesc modelCube = modelDomain.buildCubeDesc();
        modelCube.init(kylinConfig);
        return internalBuild(modelCube, modelDomain, sqls);
    }

    public CubeContext buildFromModelDesc(DataModelDesc modelDesc, QueryStats queryStats) {
        Domain modelDomain = new ModelDomainBuilder(modelDesc).build();
        CubeDesc modelCube = modelDomain.buildCubeDesc();
        modelCube.init(kylinConfig);
        return internalBuild(modelCube, modelDomain, queryStats);
    }

    public CubeContext buildFromCubeDesc(CubeDesc cubeDesc, String[] sqls) {
        return buildFromCubeDesc(cubeDesc, null, sqls);
    }

    public CubeContext buildFromCubeDesc(CubeDesc cubeDesc, ICubeStats cubeStats, String[] sqls) {
        CubeContext context = internalBuild(cubeDesc, null, sqls);
        if (cubeStats != null) {
            context.setCubeStats(cubeDesc, cubeStats);
        }
        return context;
    }

    public CubeContext buildFromCubeDesc(CubeDesc cubeDesc, ICubeStats cubeStats, QueryStats queryStats) {
        CubeContext context = internalBuild(cubeDesc, null, queryStats);
        if (cubeStats != null) {
            context.setCubeStats(cubeDesc, cubeStats);
        }
        return context;
    }

    private CubeContext internalBuild(CubeDesc initCubeDesc, Domain initDomain, String[] sqls) {
        QueryStats queryStats = null;
        Map<String, SQLResult> queryResults = null;
        Map<String, Collection<OLAPContext>> olapContexts = null;
        if (sqls != null && sqls.length > 0) {
            try (AbstractQueryRunner extractor = QueryRunnerFactory.createForCubeSuggestion(kylinConfig, sqls,
                    smartConfig.getQueryDryRunThreads(), initCubeDesc)) {
                ISQLAdvisor sqlAdvisor = new ModelBasedSQLAdvisor(initCubeDesc.getModel());
                extractor.execute();
                queryStats = extractor.getQueryStats();
                queryResults = extractor.getQueryResults();
                olapContexts = extractor.getAllOLAPContexts();
                for (Map.Entry<String, SQLResult> queryResult : queryResults.entrySet()) {
                    SQLResult sqlResult = queryResult.getValue();
                    if (sqlResult.getStatus() == SQLResult.Status.FAILED) {
                        List<SQLAdvice> advices = sqlAdvisor.provideAdvice(sqlResult,
                                olapContexts.get(queryResult.getKey()));
                        if (!advices.isEmpty()) {
                            StringBuilder msgBuilder = new StringBuilder();
                            for (SQLAdvice advice : advices) {
                                msgBuilder.append(advice.getIncapableReason()).append(' ');
                            }
                            sqlResult.setMessage(msgBuilder.toString());
                        }
                    }
                }
            } catch (Exception e) {
                logger.error("Failed to execute query stats. ", e);
            }
        }
        return internalBuild(initCubeDesc, initDomain, queryStats, queryResults);
    }

    private CubeContext internalBuild(CubeDesc initCubeDesc, Domain initDomain, QueryStats queryStats) {
        return internalBuild(initCubeDesc, initDomain, queryStats, null);
    }

    private CubeContext internalBuild(CubeDesc initCubeDesc, Domain initDomain, QueryStats queryStats,
            Map<String, SQLResult> sqlResults) {
        CubeContext context = new CubeContext(kylinConfig);

        Domain usedDomain = new DefaultDomainBuilder(smartConfig, queryStats, initCubeDesc).build();
        TableMetadataManager tableManager = TableMetadataManager.getInstance(kylinConfig);
        DataModelManager modelManager = DataModelManager.getInstance(kylinConfig);
        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(kylinConfig);
        DataModelDesc modelDesc = modelManager.getDataModelDesc(initCubeDesc.getModelName());

        // set model stats
        ModelStats modelStats = null;
        try {
            modelStats = modelStatsManager.getModelStats(modelDesc.getName());
            if (modelStats.getSingleColumnCardinality().isEmpty()) {
                // An empty modelStats will return from ModelStatsManager.getModelStats() if not existed.
                modelStats = null;
            }
        } catch (IOException e) {
            logger.error("Failed to get model stats. ", e);
        }

        // set table stats
        Map<String, TableDesc> tableDescMap = Maps.newHashMap();
        Map<String, TableExtDesc> tableExtDescMap = Maps.newHashMap();
        for (TableRef tableRef : modelDesc.getAllTables()) {
            String tblRefId = tableRef.getTableIdentity();
            TableDesc tableDesc = tableRef.getTableDesc();
            tableDescMap.put(tblRefId, tableDesc);

            TableExtDesc tableExtDesc = tableManager.getTableExt(tblRefId, modelDesc.getProject());
            if (tableExtDesc != null && !tableExtDesc.getColumnStats().isEmpty()) {
                tableExtDescMap.put(tblRefId, tableExtDesc);
            }
        }

        context.setDomain(usedDomain);
        context.setCubeName(initCubeDesc.getName());
        context.setQueryStats(queryStats);
        context.setModelStats(modelStats);
        context.setModelDesc(modelDesc);
        context.setTableDescs(tableDescMap);
        context.setTableExtDescs(tableExtDescMap);
        context.setSqlResults(sqlResults);

        return context;
    }

}
