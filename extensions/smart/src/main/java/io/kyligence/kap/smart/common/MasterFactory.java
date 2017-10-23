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

import java.util.List;
import java.util.Map;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.TableMetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.collect.Lists;

import io.kyligence.kap.smart.cube.CubeContext;
import io.kyligence.kap.smart.cube.CubeContextBuilder;
import io.kyligence.kap.smart.cube.CubeMaster;
import io.kyligence.kap.smart.cube.stats.ICubeStats;
import io.kyligence.kap.smart.model.ModelContext;
import io.kyligence.kap.smart.model.ModelContextBuilder;
import io.kyligence.kap.smart.model.ModelMaster;
import io.kyligence.kap.smart.query.QueryStats;
import io.kyligence.kap.smart.query.SQLResult;

public class MasterFactory {
    public static CubeMaster createCubeMaster(KylinConfig kylinConfig, DataModelDesc modelDesc) {
        return createCubeMaster(kylinConfig, modelDesc, new String[0]);
    }

    public static CubeMaster createCubeMaster(KylinConfig kylinConfig, DataModelDesc modelDesc, String[] sqls) {
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromModelDesc(modelDesc, sqls);
        return new CubeMaster(context);
    }

    public static CubeMaster createCubeMaster(KylinConfig kylinConfig, DataModelDesc modelDesc, QueryStats queryStats,
            List<SQLResult> sqlResults) {
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromModelDesc(modelDesc, queryStats, sqlResults);
        return new CubeMaster(context);
    }

    public static CubeMaster createCubeMaster(KylinConfig kylinConfig, CubeDesc cubeDesc, ICubeStats cubeStats,
            QueryStats queryStats, List<SQLResult> sqlResults) {
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        CubeContext context = contextBuilder.buildFromCubeDesc(cubeDesc, cubeStats, queryStats, sqlResults);
        return new CubeMaster(context);
    }

    public static List<CubeMaster> createCubeMasters(KylinConfig kylinConfig, String project, String[] sqls) {
        CubeContextBuilder contextBuilder = new CubeContextBuilder(kylinConfig);
        Map<String, CubeContext> ctxs = contextBuilder.buildFromProject(project, sqls);
        List<CubeMaster> result = Lists.newLinkedList();
        for (Map.Entry<String, CubeContext> entry : ctxs.entrySet()) {
            result.add(new CubeMaster(entry.getValue()));
        }
        return result;
    }

    public static List<ModelMaster> createModelMasters(KylinConfig kylinConfig, String project, String[] sqls) {
        return createModelMasters(kylinConfig, project, sqls, null);
    }

    public static List<ModelMaster> createModelMasters(KylinConfig kylinConfig, String project, String[] sqls,
            TableDesc factTable) {
        ModelContextBuilder contextBuilder = new ModelContextBuilder(kylinConfig, project);
        List<ModelContext> ctxs = contextBuilder.buildFromSQLs(sqls, factTable);
        List<ModelMaster> result = Lists.newLinkedList();
        for (ModelContext cxt : ctxs) {
            result.add(new ModelMaster(cxt));
        }
        return result;
    }

    public static ModelMaster createModelMaster(KylinConfig kylinConfig, String project, String[] sqls,
            String factTableName) {
        TableDesc factTable = TableMetadataManager.getInstance(kylinConfig).getTableDesc(factTableName, project);
        return createModelMaster(kylinConfig, project, sqls, factTable);
    }

    public static ModelMaster createModelMaster(KylinConfig kylinConfig, String project, String[] sqls,
            TableDesc factTable) {
        List<ModelMaster> masters = createModelMasters(kylinConfig, project, sqls, factTable);
        return CollectionUtils.isEmpty(masters) ? null : masters.get(0);
    }
}
