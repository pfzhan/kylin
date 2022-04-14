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

package io.kyligence.kap.rest.response;

import static com.google.common.collect.Lists.newArrayList;
import static org.apache.kylin.metadata.model.FunctionDesc.FUNC_COUNT;

import java.util.List;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;

public class NDataModelResponseTest extends NLocalFileMetadataTestCase {

    private static final String PROJECT = "default";

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    @Test
    public void testGetSelectedColumnsAndSimplifiedDimensionsNormal() throws Exception {
        List<NDataModel.NamedColumn> allNamedColumns = Lists.newArrayList();
        NDataModel.NamedColumn namedColumn = new NDataModel.NamedColumn();
        namedColumn.setName("PRICE1");
        namedColumn.setAliasDotColumn("TEST_KYLIN_FACT.PRICE");
        namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        allNamedColumns.add(namedColumn);

        NDataModel model = new NDataModel();
        model.setUuid("model");
        model.setProject(PROJECT);
        model.setAllNamedColumns(allNamedColumns);
        model.setAllMeasures(Lists.newArrayList(createMeasure()));
        model.setRootFactTableName("DEFAULT.TEST_KYLIN_FACT");

        createModel(model);

        NDataModelResponse modelResponse = new NDataModelResponse(model);
        modelResponse.setConfig(KylinConfig.getInstanceFromEnv());
        modelResponse.setProject(PROJECT);
        List<NDataModelResponse.SimplifiedNamedColumn> selectedColumns = modelResponse.getSelectedColumns();
        Assert.assertEquals(1, selectedColumns.size());
        List<NDataModelResponse.SimplifiedNamedColumn> namedColumns = modelResponse.getNamedColumns();
        Assert.assertEquals(1, namedColumns.size());
    }

    @Test
    public void testGetSelectedColumnsAndSimplifiedDimensionsWhenModelBroken() throws Exception {
        List<NDataModel.NamedColumn> allNamedColumns = Lists.newArrayList();
        NDataModel.NamedColumn namedColumn = new NDataModel.NamedColumn();
        namedColumn.setName("PRICE1");
        namedColumn.setAliasDotColumn("TEST_KYLIN_FACT.PRICE");
        namedColumn.setStatus(NDataModel.ColumnStatus.DIMENSION);
        allNamedColumns.add(namedColumn);

        NDataModel model = new NDataModel();
        model.setUuid("model");
        model.setProject(PROJECT);
        model.setAllNamedColumns(allNamedColumns);
        model.setAllMeasures(Lists.newArrayList(createMeasure()));
        model.setRootFactTableName("DEFAULT.TEST_KYLIN_FACT");

        createModel(model);

        NDataModelResponse modelResponse = new NDataModelResponse(model);
        modelResponse.setBroken(true);
        List<NDataModelResponse.SimplifiedNamedColumn> selectedColumns = modelResponse.getSelectedColumns();
        Assert.assertEquals(1, selectedColumns.size());
        List<NDataModelResponse.SimplifiedNamedColumn> namedColumns = modelResponse.getNamedColumns();
        Assert.assertEquals(1, namedColumns.size());
    }

    private NDataModel.Measure createMeasure() {
        NDataModel.Measure countOneMeasure = new NDataModel.Measure();
        countOneMeasure.setName("COUNT_ONE");
        countOneMeasure.setFunction(
                FunctionDesc.newInstance(FUNC_COUNT, newArrayList(ParameterDesc.newInstance("1")), "bigint"));
        countOneMeasure.setId(200001);
        return countOneMeasure;
    }

    private void createModel(NDataModel model) {
        NDataModelManager modelManager = NDataModelManager.getInstance(getTestConfig(), PROJECT);
        modelManager.createDataModelDesc(model, "root");
    }
}