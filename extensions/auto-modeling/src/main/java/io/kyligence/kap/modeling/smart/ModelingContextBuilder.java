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

package io.kyligence.kap.modeling.smart;

import java.io.IOException;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.modeling.smart.domain.Domain;
import io.kyligence.kap.modeling.smart.domain.ModelDomainBuilder;
import io.kyligence.kap.modeling.smart.query.QueryDomainBuilder;
import io.kyligence.kap.modeling.smart.query.QueryStats;
import io.kyligence.kap.modeling.smart.query.QueryStatsExtractor;
import io.kyligence.kap.source.hive.modelstats.ModelStats;
import io.kyligence.kap.source.hive.modelstats.ModelStatsManager;

public class ModelingContextBuilder {
    private static final Logger logger = LoggerFactory.getLogger(ModelingContextBuilder.class);
    private KylinConfig kylinConfig;

    public ModelingContextBuilder(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    public ModelingContext buildFromModelDesc(DataModelDesc modelDesc, String[] sqls) {
        Domain modelDomain = new ModelDomainBuilder(modelDesc).build();
        CubeDesc modelCube = modelDomain.buildCubeDesc();
        modelCube.init(kylinConfig);
        return internalBuild(modelCube, modelDomain, sqls);
    }

    public ModelingContext buildFromCubeDesc(CubeDesc cubeDesc, String[] sqls) {
        return internalBuild(cubeDesc, null, sqls);
    }

    private ModelingContext internalBuild(CubeDesc initCubeDesc, Domain initDomain, String[] sqls) {
        ModelingContext context = new ModelingContext();

        QueryStats queryStats = null;
        Domain usedDomain = initDomain;
        if (sqls != null && sqls.length > 0) {
            QueryStatsExtractor extractor = new QueryStatsExtractor(initCubeDesc, sqls);
            try {
                queryStats = extractor.extract();

                QueryDomainBuilder domainBuilder = new QueryDomainBuilder(queryStats, initCubeDesc);
                usedDomain = domainBuilder.build();
            } catch (Exception e) {
                logger.error("Failed to extract query stats. ", e);
            }
        }

        MetadataManager metadataManager = MetadataManager.getInstance(kylinConfig);
        ModelStatsManager modelStatsManager = ModelStatsManager.getInstance(kylinConfig);
        DataModelDesc modelDesc = initCubeDesc.getModel();

        ModelStats modelStats = null;
        try {
            modelStats = modelStatsManager.getModelStats(modelDesc.getName());
        } catch (IOException e) {
            logger.error("Failed to get model stats. ", e);
        }

        Map<String, TableDesc> tableDescMap = Maps.newHashMap();
        Map<String, TableExtDesc> tableExtDescMap = Maps.newHashMap();
        for (TableRef tableRef : modelDesc.getAllTables()) {
            String tblRefId = tableRef.getTableIdentity();
            TableDesc tableDesc = tableRef.getTableDesc();
            TableExtDesc tableExtDesc = metadataManager.getTableExt(tableDesc.getName());

            tableDescMap.put(tblRefId, tableDesc);
            tableExtDescMap.put(tblRefId, tableExtDesc);
        }

        context.setDomain(usedDomain);
        context.setCubeName(initCubeDesc.getName());
        context.setQueryStats(queryStats);
        context.setModelStats(modelStats);
        context.setKylinConfig(kylinConfig);
        context.setModelDesc(modelDesc);
        context.setTableDescs(tableDescMap);
        context.setTableExtDescs(tableExtDescMap);

        return context;
    }
}
