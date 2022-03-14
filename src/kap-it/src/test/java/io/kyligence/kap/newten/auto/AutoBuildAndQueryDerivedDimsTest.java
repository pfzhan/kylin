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
package io.kyligence.kap.newten.auto;

import java.io.IOException;

import org.apache.kylin.metadata.model.ModelJoinRelationTypeEnum;
import org.apache.kylin.metadata.realization.NoRealizationFoundException;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.util.scd2.SCD2CondChecker;
import io.kyligence.kap.util.ExecAndComp.CompareLevel;
import io.kyligence.kap.smart.SmartMaster;
import io.kyligence.kap.util.AccelerationContextUtil;

public class AutoBuildAndQueryDerivedDimsTest extends AutoTestBase {

    @Test
    public void testNonEquiJoinDerived() throws Exception {

        final String TEST_FOLDER = "query/sql_derived_non_equi_join";
        overwriteSystemProp("kylin.query.non-equi-join-model-enabled", "TRUE");

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        {
            //left join
            compareSCD2Derived(TEST_FOLDER, 0, JoinType.LEFT);
            //inner join
            compareSCD2Derived(TEST_FOLDER, 0, JoinType.INNER);
        }

        for (int i = 2; i < 5; i++) {
            Assert.assertFalse(SCD2CondChecker.INSTANCE.isScd2Model(proposeSmartModel(TEST_FOLDER, i, JoinType.LEFT)));
        }

        {
            //left join
            compareSCD2Derived(TEST_FOLDER, 5, JoinType.LEFT);
            //inner join
            compareSCD2Derived(TEST_FOLDER, 5, JoinType.INNER);
        }
    }

    @Test
    public void testEquiDerivedColumnDisabled() throws Exception {
        final String TEST_FOLDER = "query/sql_derived_equi_join";

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        NDataModel model = proposeSmartModel(TEST_FOLDER, 0, JoinType.LEFT);
        model.getJoinTables().get(1).setJoinRelationTypeEnum(ModelJoinRelationTypeEnum.MANY_TO_MANY);

        NDataModelManager.getInstance(getTestConfig(), getProject()).updateDataModelDesc(model);

        try {
            compareDerivedWithInitialModel(TEST_FOLDER, 0, JoinType.LEFT);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e) instanceof NoRealizationFoundException);
        }
    }

    @Test
    public void testDerivedPkFormFk_LeftJoin() throws Exception {
        final String TEST_FOLDER = "query/sql_derived_equi_join";

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        proposeSmartModel(TEST_FOLDER, 2, JoinType.LEFT);

        compareDerivedWithInitialModel(TEST_FOLDER, 2, JoinType.LEFT);
    }

    @Test
    public void testDerivedPkFormFk_InnerJoin() throws Exception {
        final String TEST_FOLDER = "query/sql_derived_equi_join";

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        proposeSmartModel(TEST_FOLDER, 2, JoinType.INNER);

        compareDerivedWithInitialModel(TEST_FOLDER, 2, JoinType.INNER);
    }

    @Test
    public void testDerivedFkFromPk_LeftJoin() throws Exception {
        final String TEST_FOLDER = "query/sql_derived_equi_join";

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        proposeSmartModel(TEST_FOLDER, 4, JoinType.LEFT);

        try {
            compareDerivedWithInitialModel(TEST_FOLDER, 4, JoinType.LEFT);
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(Throwables.getRootCause(e) instanceof NoRealizationFoundException);
        }
    }

    @Test
    public void testDerivedFkFromPk_InnerJoin() throws Exception {
        final String TEST_FOLDER = "query/sql_derived_equi_join";

        AccelerationContextUtil.transferProjectToSemiAutoMode(getTestConfig(), getProject());

        proposeSmartModel(TEST_FOLDER, 4, JoinType.INNER);

        compareDerivedWithInitialModel(TEST_FOLDER, 4, JoinType.INNER);
    }

    private void compareDerivedWithInitialModel(String testFolder, int startIndex, JoinType joinType) throws Exception {

        TestScenario derivedQuerys = new TestScenario(CompareLevel.SAME, testFolder, joinType, startIndex + 1,
                startIndex + 2);
        collectQueries(Lists.newArrayList(derivedQuerys));
        buildAndCompare(derivedQuerys);
    }

    private void compareSCD2Derived(String testFolder, int startIndex, JoinType joinType) throws Exception {
        NDataModel model = proposeSmartModel(testFolder, startIndex, joinType);
        Assert.assertTrue(SCD2CondChecker.INSTANCE.isScd2Model(model));
        compareDerivedWithInitialModel(testFolder, startIndex, joinType);
    }

    private NDataModel proposeSmartModel(String testFolder, int startIndex, JoinType joinType) throws IOException {
        SmartMaster smartMaster = proposeWithSmartMaster(getProject(), Lists
                .newArrayList(new TestScenario(CompareLevel.NONE, testFolder, joinType, startIndex, startIndex + 1)));

        Assert.assertEquals(smartMaster.getContext().getModelContexts().size(), 1);
        return smartMaster.getContext().getModelContexts().get(0).getTargetModel();
    }
}
