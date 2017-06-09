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

package io.kyligence.kap.rest.service;

import java.io.IOException;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import io.kyligence.kap.modeling.smart.ModelingMaster;
import io.kyligence.kap.modeling.smart.ModelingMasterFactory;
import io.kyligence.kap.modeling.smart.cube.CubeLog;
import io.kyligence.kap.modeling.smart.cube.CubeLogManager;
import io.kyligence.kap.modeling.smart.query.QueryStats;

@Component("kapSuggestionService")
public class KapSuggestionService extends BasicService {
    public void saveSampleSqls(String modelName, String cubeName, List<String> sampleSqls) throws Exception {
        CubeLogManager cubeLogManager = CubeLogManager.getInstance(getConfig());
        CubeLog cubeLog = cubeLogManager.getCubeLog(cubeName);

        if (sampleSqls.size() == 0)
            return;

        if (!isSampleSqlUpdated(sampleSqls, cubeLog.getSampleSqls()))
            return;

        String[] sqlArray = new String[sampleSqls.size()];
        sampleSqls.toArray(sqlArray);

        DataModelDesc dataModelDesc = MetadataManager.getInstance(getConfig()).getDataModelDesc(modelName);
        ModelingMaster modelingMaster = ModelingMasterFactory.create(getConfig(), dataModelDesc, sqlArray);

        cubeLog.setQueryStats(modelingMaster.getContext().getQueryStats());
        cubeLog.setSampleSqls(sampleSqls);
        cubeLogManager.saveCubeLog(cubeLog);
    }

    public List<String> getSampleSqls(String cubeName) throws IOException {
        CubeLogManager cubeLogManager = CubeLogManager.getInstance(getConfig());
        CubeLog cubeLog = cubeLogManager.getCubeLog(cubeName);
        return cubeLog.getSampleSqls();
    }

    public CubeDesc optimizeCube(CubeDesc cubeDesc) throws IOException {
        try (SetThreadName ignored = new SetThreadName("Suggestion %s", Long.toHexString(Thread.currentThread().getId()))) {

            ModelingMaster master = getModelingMaster(cubeDesc.getName(), cubeDesc.getModelName());
            CubeDesc rowkeyCube = master.proposeRowkey(cubeDesc);
            CubeDesc aggGroupCube = master.proposeAggrGroup(rowkeyCube);
            CubeDesc configOverrideCube = master.proposeConfigOverride(aggGroupCube);
            return configOverrideCube;
        }
    }

    public CubeDesc proposeDimensions(String cubeName, String modelName) throws IOException {
        try (SetThreadName ignored = new SetThreadName("Suggestion %s", Long.toHexString(Thread.currentThread().getId()))) {
            ModelingMaster master = getModelingMaster(cubeName, modelName);
            CubeDesc dimMeasCube = master.proposeDerivedDimensions(master.proposeInitialCube());
            return dimMeasCube;
        }
    }

    private ModelingMaster getModelingMaster(String cubeName, String modelName) throws IOException {
        KylinConfig config = getConfig();
        CubeLogManager cubeLogManager = CubeLogManager.getInstance(config);
        QueryStats queryStats = cubeLogManager.getCubeLog(cubeName).getQueryStats();
        DataModelDesc dataModelDesc = MetadataManager.getInstance(config).getDataModelDesc(modelName);
        ModelingMaster modelingMaster = ModelingMasterFactory.create(config, dataModelDesc);

        if (null != queryStats)
            modelingMaster.getContext().setQueryStats(queryStats);
        return modelingMaster;
    }

    private boolean isSampleSqlUpdated(List<String> newSqls, List<String> oldSqls) {
        if (newSqls.size() == oldSqls.size() && newSqls.containsAll(oldSqls))
            return false;
        return true;
    }

}
