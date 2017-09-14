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
import java.util.Collection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.MetadataManager;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.smart.common.MasterFactory;
import io.kyligence.kap.smart.cube.CubeContext;
import io.kyligence.kap.smart.cube.CubeMaster;
import io.kyligence.kap.smart.cube.CubeOptimizeLog;
import io.kyligence.kap.smart.cube.CubeOptimizeLogManager;
import io.kyligence.kap.smart.cube.ModelOptimizeLog;
import io.kyligence.kap.smart.cube.ModelOptimizeLogManager;
import io.kyligence.kap.smart.model.ModelMaster;
import io.kyligence.kap.smart.query.QueryStats;
import io.kyligence.kap.smart.query.SQLResult;
import io.kyligence.kap.smart.query.validator.RawModelSQLValidator;
import io.kyligence.kap.smart.query.validator.SQLValidateResult;

@Component("kapSuggestionService")
public class KapSuggestionService extends BasicService {
    public void saveSampleSqls(String modelName, String cubeName, List<String> sampleSqls) throws Exception {
        CubeOptimizeLogManager cubeOptimizeLogManager = CubeOptimizeLogManager.getInstance(getConfig());
        CubeOptimizeLog cubeOptimizeLog = cubeOptimizeLogManager.getCubeOptimizeLog(cubeName);

        if (!isSampleSqlUpdated(sampleSqls, cubeOptimizeLog.getSampleSqls()) && cubeOptimizeLog.isValid())
            return;

        String[] sqlArray = new String[sampleSqls.size()];
        sampleSqls.toArray(sqlArray);

        DataModelDesc dataModelDesc = MetadataManager.getInstance(getConfig()).getDataModelDesc(modelName);
        CubeMaster modelingMaster = MasterFactory.createCubeMaster(getConfig(), dataModelDesc, sqlArray);

        CubeContext modelingContext = modelingMaster.getContext();
        cubeOptimizeLog.setSampleSqls(sampleSqls);
        cubeOptimizeLog.setQueryStats(modelingContext.getQueryStats());
        cubeOptimizeLog.setSqlResult(modelingContext.getSQLResultList(sampleSqls));
        cubeOptimizeLogManager.saveCubeOptimizeLog(cubeOptimizeLog);
    }

    public List<SQLResult> checkSampleSqls(String modelName, String cubeName, List<String> sampleSqls)
            throws Exception {
        if (sampleSqls.size() == 0)
            return Lists.newArrayList();

        CubeOptimizeLogManager cubeOptimizeLogManager = CubeOptimizeLogManager.getInstance(getConfig());
        CubeOptimizeLog cubeOptimizeLog = cubeOptimizeLogManager.getCubeOptimizeLog(cubeName);

        if (!isSampleSqlUpdated(sampleSqls, cubeOptimizeLog.getSampleSqls()) && cubeOptimizeLog.isValid())
            return cubeOptimizeLog.getSqlResult();

        String[] sqlArray = new String[sampleSqls.size()];
        sampleSqls.toArray(sqlArray);

        DataModelDesc dataModelDesc = MetadataManager.getInstance(getConfig()).getDataModelDesc(modelName);
        CubeMaster modelingMaster = MasterFactory.createCubeMaster(getConfig(), dataModelDesc, sqlArray);

        CubeContext modelingContext = modelingMaster.getContext();
        return modelingContext.getSQLResultList(sampleSqls);
    }

    public CubeOptimizeLog getCubeOptLog(String cubeName) throws IOException {
        CubeOptimizeLogManager cubeOptimizeLogManager = CubeOptimizeLogManager.getInstance(getConfig());
        CubeOptimizeLog cubeOptimizeLog = cubeOptimizeLogManager.getCubeOptimizeLog(cubeName);
        return cubeOptimizeLog;
    }

    public CubeDesc proposeAggGroups(CubeDesc cubeDesc) throws IOException {
        try (SetThreadName ignored = new SetThreadName("Suggestion %s",
                Long.toHexString(Thread.currentThread().getId()))) {

            CubeMaster master = getModelingMaster(cubeDesc.getName(), cubeDesc.getModelName(),
                    cubeDesc.getOverrideKylinProps());
            CubeDesc rowkeyCube = master.proposeRowkey(cubeDesc);
            CubeDesc aggGroupCube = master.proposeAggrGroup(rowkeyCube);
            return aggGroupCube;
        }
    }

    public Collection<String> getQueryDimensions(String cubeName) throws IOException {
        CubeOptimizeLog cubeOptLog = getCubeOptLog(cubeName);
        return cubeOptLog.getQueryStats().getAppears().keySet();
    }

    public CubeDesc proposeDimAndMeasures(String cubeName, String modelName) throws IOException {
        try (SetThreadName ignored = new SetThreadName("Suggestion %s",
                Long.toHexString(Thread.currentThread().getId()))) {
            CubeMaster master = getModelingMaster(cubeName, modelName, null);
            CubeDesc dimMeasCube = master.proposeDerivedDimensions(master.proposeInitialCube());
            return dimMeasCube;
        }
    }

    public List<SQLValidateResult> validateSqls(String project, String modelName, String factTable,
            List<String> sqls) throws IOException {
        try (SetThreadName ignored = new SetThreadName("Suggestion %s",
                Long.toHexString(Thread.currentThread().getId()))) {

            ModelOptimizeLog modelOptimizeLog = updateModelOptimizeLog(project, modelName, factTable, sqls);
            return modelOptimizeLog.getSqlValidateResult();
        }
    }

    public List<String> getModelSqls(String modelName) throws IOException {

        ModelOptimizeLog modelOptimizeLog = getModelOptimizeLog(modelName);
        if (null == modelOptimizeLog) {
            return new ArrayList<>();
        }
        return modelOptimizeLog.getSampleSqls();
    }

    public DataModelDesc proposeDataModel(String project, String modelName, String factTable, List<String> sqls)
            throws IOException {
        try (SetThreadName ignored = new SetThreadName("Suggestion %s",
                Long.toHexString(Thread.currentThread().getId()))) {

            ModelOptimizeLog optimizeLog = updateModelOptimizeLog(project, modelName, factTable, sqls);

            String[] sqlArray = optimizeLog.getValidatedSqls().toArray(new String[0]);
            KylinConfig config = getConfig();
            ModelMaster master = MasterFactory.createModelMaster(config, project, sqlArray, factTable);
            master.getContext().setModelName(modelName);
            DataModelDesc modelDesc = master.proposeAll();
            return modelDesc;
        }
    }

    private ModelOptimizeLog updateModelOptimizeLog(String project, String modelName, String factTable,
            List<String> sqls) throws IOException {
        boolean isUpdated = true;
        ModelOptimizeLog modelOptimizeLog = getModelOptimizeLog(modelName);

        if (modelOptimizeLog != null) {
            isUpdated = isSampleSqlUpdated(sqls, modelOptimizeLog.getSampleSqls());
        }

        if (!isUpdated) {
            return modelOptimizeLog;
        }

        // Do validation to get updated
        RawModelSQLValidator validator = new RawModelSQLValidator(getConfig(), project, factTable);
        Map<String, SQLValidateResult> results = validator.batchValidate(sqls);
        List<SQLValidateResult> orderedResult = new ArrayList<>(sqls.size());
        for (String sql : sqls) {
            orderedResult.add(results.get(sql));
        }

        if (modelOptimizeLog == null) {
            modelOptimizeLog = new ModelOptimizeLog();
            modelOptimizeLog.setModelName(modelName);
        }
        modelOptimizeLog.setSampleSqls(sqls);
        modelOptimizeLog.setSqlValidateResult(orderedResult);
        saveModelOptimizeLog(modelOptimizeLog);

        return modelOptimizeLog;
    }

    private ModelOptimizeLog getModelOptimizeLog(String modelName) throws IOException {
        ModelOptimizeLogManager modelOptimizeLogManager = ModelOptimizeLogManager.getInstance(getConfig());
        return modelOptimizeLogManager.getModelOptimizeLog(modelName);

    }

    private void saveModelOptimizeLog(ModelOptimizeLog modelOptimizeLog) throws IOException {
        ModelOptimizeLogManager modelOptimizeLogManager = ModelOptimizeLogManager.getInstance(getConfig());
        modelOptimizeLogManager.saveModelOptimizeLog(modelOptimizeLog);
    }

    private CubeMaster getModelingMaster(String cubeName, String modelName, Map<String, String> props)
            throws IOException {
        Map<String, String> overrideProps = Maps.newHashMap();
        ProjectInstance projectInstance = ProjectManager.getInstance(getConfig()).getProjectOfModel(modelName);
        if (projectInstance != null && projectInstance.getOverrideKylinProps() != null) {
            overrideProps.putAll(projectInstance.getOverrideKylinProps());
        }
        if (props != null) {
            overrideProps.putAll(props);
        }

        KylinConfig config = KylinConfigExt.createInstance(getConfig(), overrideProps);
        CubeOptimizeLogManager cubeOptimizeLogManager = CubeOptimizeLogManager.getInstance(config);
        QueryStats queryStats = cubeOptimizeLogManager.getCubeOptimizeLog(cubeName).getQueryStats();
        DataModelDesc dataModelDesc = MetadataManager.getInstance(config).getDataModelDesc(modelName);
        CubeMaster modelingMaster = MasterFactory.createCubeMaster(config, dataModelDesc, queryStats);
        return modelingMaster;
    }

    private boolean isSampleSqlUpdated(List<String> newSqls, List<String> oldSqls) {
        if (newSqls.size() == oldSqls.size() && newSqls.containsAll(oldSqls))
            return false;
        return true;
    }

}
