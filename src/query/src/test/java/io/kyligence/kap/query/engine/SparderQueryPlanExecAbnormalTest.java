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
package io.kyligence.kap.query.engine;

import org.apache.calcite.rel.RelNode;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.common.ForceToTieredStorage;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.exception.KylinException;
import org.apache.spark.SparkException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import io.kyligence.kap.query.engine.exec.ExecuteResult;
import io.kyligence.kap.query.engine.exec.sparder.QueryEngine;
import io.kyligence.kap.query.engine.exec.sparder.SparderQueryPlanExec;
import io.kyligence.kap.query.engine.meta.MutableDataContext;
import lombok.SneakyThrows;

import java.sql.SQLException;

public class SparderQueryPlanExecAbnormalTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    static class ThrowForceToTieredStorageException implements ThrownExceptionEngine.EngineAction {
        ThrowForceToTieredStorageException() {
        }

        @SneakyThrows
        @Override
        public boolean apply() {
            return ExceptionUtils.rethrow(new SQLException(QueryContext.ROUTE_USE_FORCEDTOTIEREDSTORAGE));
        }
    }

    static class ThrowExceptionAtFirstTime implements ThrownExceptionEngine.EngineAction {
        public int callNumber = 0;
        private final boolean setSecondStorageUsageMap;

        ThrowExceptionAtFirstTime(boolean setSecondStorageUsageMap) {
            this.setSecondStorageUsageMap = setSecondStorageUsageMap;
        }

        @SneakyThrows
        @Override
        public boolean apply() {
            callNumber++;
            if (callNumber == 1) {
                if (setSecondStorageUsageMap) {
                    QueryContext.current().getSecondStorageUsageMap().put(1L, true);
                }
                throw new SparkException("");
            }
            return true;
        }
    }

    static class TestSparderQueryPlanExec extends SparderQueryPlanExec {
        private QueryEngine engine;

        TestSparderQueryPlanExec(QueryEngine engine) {
            this.engine = engine;
        }

        public void updateEngine(QueryEngine engine) {
            this.engine = engine;
        }

        @Override
        public ExecuteResult executeToIterable(RelNode rel, MutableDataContext dataContext) {
            return internalCompute(engine, dataContext, rel);
        }
    }

    @After
    public void tearDown() {
        QueryContext.reset();
    }

    @Test
    public void testQueryRouteWithSecondStorage() {
        ThrowExceptionAtFirstTime throwExceptionAtFirstTime = new ThrowExceptionAtFirstTime(true);
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);
        QueryContext.current().setRetrySecondStorage(false);
        Assert.assertNull(exec.executeToIterable(null, null).getRows());
        Assert.assertEquals(2, throwExceptionAtFirstTime.callNumber);
        Assert.assertTrue(QueryContext.current().isForceTableIndex());
        Assert.assertTrue(QueryContext.current().getSecondStorageUsageMap().isEmpty());

        //Now QueryContext.current().isForceTableIndex() == true
        ThrowExceptionAtFirstTime throwExceptionAtFirstTime2 = new ThrowExceptionAtFirstTime(true);
        ThrownExceptionEngine engine2 = new ThrownExceptionEngine(throwExceptionAtFirstTime2);
        exec.updateEngine(engine2);

        thrown.expect(SparkException.class);
        try {
            exec.executeToIterable(null, null);
        } finally {
            Assert.assertEquals(1, throwExceptionAtFirstTime2.callNumber);
            Assert.assertTrue(QueryContext.current().isForceTableIndex());
            Assert.assertTrue(QueryContext.current().getSecondStorageUsageMap().size() > 0);
        }
    }

    @Test
    public void testQueryRouteWithoutSecondStorage() {
        ThrowExceptionAtFirstTime throwExceptionAtFirstTime = new ThrowExceptionAtFirstTime(false);
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);

        thrown.expect(SparkException.class);
        try {
            exec.executeToIterable(null, null);
        } finally {
            Assert.assertEquals(1, throwExceptionAtFirstTime.callNumber);
            Assert.assertFalse(QueryContext.current().isForceTableIndex());
            Assert.assertEquals(0, QueryContext.current().getSecondStorageUsageMap().size());
        }
    }

    @Test(expected = SQLException.class)
    public void testQueryRouteWithForceToTieredStoragePushDown() {
        ThrowForceToTieredStorageException throwExceptionAtFirstTime = new ThrowForceToTieredStorageException();
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);
        QueryContext.current().setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN);
        Assert.assertNull(exec.executeToIterable(null, null).getRows());
    }

    @Test(expected = KylinException.class)
    public void testQueryRouteWithForceToTieredStorageReturn() {
        ThrowForceToTieredStorageException throwExceptionAtFirstTime = new ThrowForceToTieredStorageException();
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);
        QueryContext.current().setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_RETURN);
        Assert.assertNull(exec.executeToIterable(null, null).getRows());
    }

    @Test(expected = KylinException.class)
    public void testQueryRouteWithForceToTieredStorageInvalid() {
        ThrowForceToTieredStorageException throwExceptionAtFirstTime = new ThrowForceToTieredStorageException();
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);
        QueryContext.current().setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TO_PUSH_DOWN);
        QueryContext.current().setForceTableIndex(true);
        Assert.assertNull(exec.executeToIterable(null, null).getRows());
    }

    @Test(expected = KylinException.class)
    public void testQueryRouteWithForceToTieredStorageOther() {
        ThrowForceToTieredStorageException throwExceptionAtFirstTime = new ThrowForceToTieredStorageException();
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);
        QueryContext.current().setForcedToTieredStorage(ForceToTieredStorage.CH_FAIL_TAIL);
        Assert.assertNull(exec.executeToIterable(null, null).getRows());
    }

}
