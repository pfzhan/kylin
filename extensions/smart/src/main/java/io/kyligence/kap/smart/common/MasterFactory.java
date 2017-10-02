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

package io.kyligence.kap.smart.common;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;

import io.kyligence.kap.smart.cube.CubeContext;
import io.kyligence.kap.smart.cube.CubeContextBuilder;
import io.kyligence.kap.smart.cube.CubeMaster;
import io.kyligence.kap.smart.cube.stats.ICubeStats;
import io.kyligence.kap.smart.model.ModelContext;
import io.kyligence.kap.smart.model.ModelContextBuilder;
import io.kyligence.kap.smart.model.ModelMaster;
import io.kyligence.kap.smart.query.QueryStats;

public class MasterFactory {
    public static CubeMaster createCubeMaster(KylinConfig kylinConfig, DataModelDesc modelDesc) {
        return createCubeMaster(kylinConfig, modelDesc, new String[0]);
    }

    public static CubeMaster createCubeMaster(KylinConfig kylinConfig, CubeDesc cubeDesc) {
        return createCubeMaster(kylinConfig, cubeDesc, new String[0]);
    }

    public static CubeMaster createCubeMaster(KylinConfig kylinConfig, DataModelDesc modelDesc, String[] sqls) {
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromModelDesc(modelDesc, sqls);
        return new CubeMaster(context);
    }

    public static CubeMaster createCubeMaster(KylinConfig kylinConfig, DataModelDesc modelDesc, QueryStats queryStats) {
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromModelDesc(modelDesc, queryStats);
        return new CubeMaster(context);
    }

    public static CubeMaster createCubeMaster(KylinConfig kylinConfig, CubeDesc cubeDesc, String[] sqls) {
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromCubeDesc(cubeDesc, sqls);
        return new CubeMaster(context);
    }

    public static CubeMaster createCubeMaster(KylinConfig kylinConfig, CubeDesc cubeDesc, ICubeStats cubeStats,
            QueryStats queryStats) {
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromCubeDesc(cubeDesc, cubeStats, queryStats);
        return new CubeMaster(context);
    }

    private static Map<TableDesc, ModelMaster> createModelMasterMap(KylinConfig kylinConfig, String project,
            String[] sqls) {
        Map<TableDesc, ModelMaster> resultMap = Maps.newHashMap();

        ModelContextBuilder contextBuilder = new ModelContextBuilder(kylinConfig, project);
        Map<TableDesc, ModelContext> contextMap = contextBuilder.buildFromSQLs(sqls);

        for (Entry<TableDesc, ModelContext> entry : contextMap.entrySet()) {
            resultMap.put(entry.getKey(), new ModelMaster(entry.getValue()));
        }

        return resultMap;
    }

    public static Collection<ModelMaster> createModelMasters(KylinConfig kylinConfig, String project, String[] sqls) {
        return createModelMasterMap(kylinConfig, project, sqls).values();
    }

    public static ModelMaster createModelMaster(KylinConfig kylinConfig, String project, String[] sqls,
            String factTableName) {
        TableDesc factTable = TableMetadataManager.getInstance(kylinConfig).getTableDesc(factTableName, project);
        return createModelMaster(kylinConfig, project, sqls, factTable);
    }

    public static ModelMaster createModelMaster(KylinConfig kylinConfig, String project, String[] sqls,
            TableDesc factTable) {
        Map<TableDesc, ModelMaster> masterMap = createModelMasterMap(kylinConfig, project, sqls);
        ModelMaster master = masterMap.get(factTable);
        Preconditions.checkArgument(master != null, "No SQL used fact table " + factTable.getIdentity());
        return masterMap.get(factTable);
    }
}
