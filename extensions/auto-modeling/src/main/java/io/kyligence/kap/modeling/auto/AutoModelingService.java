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

package io.kyligence.kap.modeling.auto;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TableRef;

import io.kyligence.kap.modeling.auto.proposer.IProposer;
import io.kyligence.kap.modeling.auto.proposer.ModelProposer;
import io.kyligence.kap.modeling.auto.tuner.PhysicalTuner;
import io.kyligence.kap.source.hive.modelstats.ModelStats;
import io.kyligence.kap.source.hive.modelstats.ModelStatsManager;

/**
 * 
 * Entry of Auto Cubing Functionality
 * 
 */
public class AutoModelingService {

    private static AutoModelingService INSTANCE = null;

    private KylinConfig config;
    private MetadataManager metadataManager;
    private ModelStatsManager modelStatsManager;

    private AutoModelingService() {
        config = KylinConfig.getInstanceFromEnv();
        metadataManager = MetadataManager.getInstance(config);
        modelStatsManager = ModelStatsManager.getInstance(config);
    }

    public synchronized static AutoModelingService getInstance() {
        if (INSTANCE == null) {
            INSTANCE = new AutoModelingService();
        }
        return INSTANCE;
    }

    public CubeDesc generateCube(String modelName, String[] queries, String project) throws Exception {
        DataModelDesc model = metadataManager.getDataModelDesc(modelName);
        ModelStats modelStats = modelStatsManager.getModelStats(modelName);
        if (model == null) {
            return null;
        }
        if (modelStats == null) {
            return null;
        }

        // Phase 1 - Propose: Model => Initial Cube; Query Dry-run
        IProposer proposer = new ModelProposer(model);
        CubeDesc workCubeDesc = proposer.propose(queries, project);
        if (workCubeDesc == null) {
            throw new RuntimeException("Failed to create cube from dry run.");
        }
        workCubeDesc.getOverrideKylinProps().put("kylin.cube.aggrgroup.max-combination", Integer.toString(Integer.MAX_VALUE - 1));
        workCubeDesc.setEngineType(config.getDefaultCubeEngine());
        workCubeDesc.setStorageType(config.getDefaultStorageEngine());
        workCubeDesc.init(config);

        // Phase 2 - Tune: physical optimization
        ModelingContextBuilder contextBuilder = new ModelingContextBuilder();
        contextBuilder.setKylinConfig(config);
        contextBuilder.setModelDesc(model);
        contextBuilder.setModelStats(modelStats);
        for (TableRef table : model.getAllTables()) {
            String tableName = table.getTableIdentity();
            TableDesc tableDesc = metadataManager.getTableDesc(tableName);
            TableExtDesc tableExtDesc = metadataManager.getTableExt(tableName);
            contextBuilder.addTable(tableDesc, tableExtDesc);
        }
        ModelingContext context = contextBuilder.build();
        PhysicalTuner physicalTuner = new PhysicalTuner(context);
        physicalTuner.optimize(workCubeDesc);

        return workCubeDesc;
    }
}
