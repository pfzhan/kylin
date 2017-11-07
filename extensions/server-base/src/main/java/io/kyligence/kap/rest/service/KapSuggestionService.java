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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections.MapUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.util.SetThreadName;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.DataModelManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.project.ProjectManager;
import org.apache.kylin.rest.service.BasicService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.KapModel;
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

    @Autowired
    @Qualifier("kapCubeService")
    private KapCubeService kapCubeService;

    public void saveSampleSqls(String modelName, String cubeName, List<String> sampleSqls) throws Exception {
        CubeOptimizeLogManager cubeOptimizeLogManager = CubeOptimizeLogManager.getInstance(getConfig());
        CubeOptimizeLog cubeOptimizeLog = cubeOptimizeLogManager.getCubeOptimizeLog(cubeName);

        String[] sqlArray = new String[sampleSqls.size()];
        sampleSqls.toArray(sqlArray);

        DataModelDesc dataModelDesc = DataModelManager.getInstance(getConfig()).getDataModelDesc(modelName);
        CubeMaster modelingMaster = MasterFactory.createCubeMaster(getConfig(), dataModelDesc, sqlArray);

        CubeContext modelingContext = modelingMaster.getContext();
        cubeOptimizeLog.setSampleSqls(sampleSqls);
        cubeOptimizeLog.setQueryStats(modelingContext.getQueryStats());
        cubeOptimizeLog.setSqlResult(Lists.newArrayList(modelingContext.getSqlResults()));
        cubeOptimizeLogManager.saveCubeOptimizeLog(cubeOptimizeLog);
    }

    public List<SQLResult> checkSampleSqls(String modelName, String cubeName, List<String> sampleSqls)
            throws Exception {
        if (sampleSqls.size() == 0)
            return Lists.newArrayList();

        String[] sqlArray = new String[sampleSqls.size()];
        sampleSqls.toArray(sqlArray);

        DataModelDesc dataModelDesc = DataModelManager.getInstance(getConfig()).getDataModelDesc(modelName);
        CubeMaster modelingMaster = MasterFactory.createCubeMaster(getConfig(), dataModelDesc, sqlArray);

        CubeContext modelingContext = modelingMaster.getContext();
        return modelingContext.getSqlResults();
    }

    public CubeOptimizeLog getCubeOptLog(String cubeName) throws IOException {
        CubeOptimizeLogManager cubeOptimizeLogManager = CubeOptimizeLogManager.getInstance(getConfig());
        CubeOptimizeLog cubeOptimizeLog = cubeOptimizeLogManager.getCubeOptimizeLog(cubeName);
        return cubeOptimizeLog;
    }

    public CubeDesc proposeAggGroups(CubeDesc cubeDesc) throws IOException {
        kapCubeService.validateMPDimensions(cubeDesc,
                (KapModel) getDataModelManager().getDataModelDesc(cubeDesc.getModelName()));

        try (SetThreadName ignored = new SetThreadName("Suggestion %s",
                Long.toHexString(Thread.currentThread().getId()))) {

            CubeMaster master = getCubeMaster(cubeDesc.getName(), cubeDesc.getModelName(),
                    cubeDesc.getOverrideKylinProps());
            CubeDesc rowkeyCube = master.proposeRowkey(cubeDesc);
            CubeDesc aggGroupCube = master.proposeAggrGroup(rowkeyCube);
            return aggGroupCube;
        }
    }

    public Collection<String> suggestDimensionColumns(String cubeName, String modelName) throws IOException {
        Set<String> results = Sets.newHashSet();
        CubeOptimizeLog cubeOptLog = getCubeOptLog(cubeName);
        if (cubeOptLog != null && cubeOptLog.getQueryStats() != null
                && MapUtils.isNotEmpty(cubeOptLog.getQueryStats().getAppears())) {
            results.addAll(cubeOptLog.getQueryStats().getAppears().keySet());
        }
        KapModel kapModel = (KapModel) getDataModelManager().getDataModelDesc(modelName);
        if (kapModel.getMpColStrs() != null) {
            Collections.addAll(results, kapModel.getMpColStrs());
        }
        return results;
    }

    public CubeDesc proposeDimAndMeasures(String cubeName, String modelName) throws IOException {
        try (SetThreadName ignored = new SetThreadName("Suggestion %s",
                Long.toHexString(Thread.currentThread().getId()))) {
            CubeMaster master = getCubeMaster(cubeName, modelName, null);
            CubeDesc dimMeasCube = master.proposeDerivedDimensions(master.proposeInitialCube());
            return dimMeasCube;
        }
    }

    public List<SQLValidateResult> validateModelSqls(String project, String modelName, String factTable,
            List<String> sqls) throws IOException {
        try (SetThreadName ignored = new SetThreadName("Suggestion %s",
                Long.toHexString(Thread.currentThread().getId()))) {

            ModelOptimizeLog modelOptimizeLog = updateModelOptimizeLog(project, modelName, factTable, sqls);
            return modelOptimizeLog.getSqlValidateResult();
        }
    }

    public Map<String, SQLValidateResult> getModelSqls(String modelName) throws IOException {

        Map<String, SQLValidateResult> sqlResults = new HashMap<>();

        ModelOptimizeLog modelOptimizeLog = getModelOptimizeLog(modelName);
        if (null == modelOptimizeLog) {
            return sqlResults;
        }

        List<String> sqls = modelOptimizeLog.getSampleSqls();
        if (null == sqls || sqls.isEmpty()) {
            return sqlResults;
        }

        List<SQLValidateResult> results = modelOptimizeLog.getSqlValidateResult();
        for (int i = 0; i < sqls.size(); i++) {
            String sql = sqls.get(i);
            SQLValidateResult result = (null != results && i < results.size()) ? results.get(i) : null;
            sqlResults.put(sql, result);
        }

        return sqlResults;
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
        ModelOptimizeLog modelOptimizeLog = getModelOptimizeLog(modelName);

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

    public ModelOptimizeLog getModelOptimizeLog(String modelName) throws IOException {
        ModelOptimizeLogManager modelOptimizeLogManager = ModelOptimizeLogManager.getInstance(getConfig());
        return modelOptimizeLogManager.getModelOptimizeLog(modelName);

    }

    private void saveModelOptimizeLog(ModelOptimizeLog modelOptimizeLog) throws IOException {
        ModelOptimizeLogManager modelOptimizeLogManager = ModelOptimizeLogManager.getInstance(getConfig());
        modelOptimizeLogManager.saveModelOptimizeLog(modelOptimizeLog);
    }

    private CubeMaster getCubeMaster(String cubeName, String modelName, Map<String, String> props) throws IOException {
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
        CubeOptimizeLog cubeOptLog = cubeOptimizeLogManager.getCubeOptimizeLog(cubeName);
        QueryStats queryStats = cubeOptLog.getQueryStats();
        List<SQLResult> sqlResults = cubeOptLog.getSqlResult();
        DataModelDesc dataModelDesc = DataModelManager.getInstance(config).getDataModelDesc(modelName);
        return MasterFactory.createCubeMaster(config, dataModelDesc, queryStats, sqlResults);
    }
}
