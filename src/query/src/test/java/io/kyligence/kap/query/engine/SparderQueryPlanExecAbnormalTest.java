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


import io.kyligence.kap.query.engine.exec.sparder.QueryEngine;
import io.kyligence.kap.query.engine.exec.sparder.SparderQueryPlanExec;
import io.kyligence.kap.query.engine.meta.MutableDataContext;
import lombok.SneakyThrows;
import org.apache.calcite.rel.RelNode;
import org.apache.kylin.common.QueryContext;
import org.apache.spark.SparkException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;

import org.junit.rules.ExpectedException;
import scala.runtime.AbstractFunction0;

import java.util.List;

public class SparderQueryPlanExecAbnormalTest {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    static class ThrowExceptionAtFirstTime extends AbstractFunction0<Object> {
        public int callNumber = 0;
        private final boolean setSecondStorageUsageMap;
        ThrowExceptionAtFirstTime(boolean setSecondStorageUsageMap) {
            this.setSecondStorageUsageMap = setSecondStorageUsageMap;
        }
        @SneakyThrows
        @Override
        public Boolean apply() {
            callNumber++;
            if(callNumber == 1) {
                if(setSecondStorageUsageMap) {
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
        public List<List<String>> execute(RelNode rel, MutableDataContext dataContext) {
            return internalCompute(engine, dataContext, rel);
        }
    }

    @After
    public void tearDown(){
        QueryContext.reset();
    }

    @Test
    public void testQueryRouteWithSecondStorage() {
        ThrowExceptionAtFirstTime throwExceptionAtFirstTime = new ThrowExceptionAtFirstTime(true);
        ThrownExceptionEngine engine = new ThrownExceptionEngine(throwExceptionAtFirstTime);
        TestSparderQueryPlanExec exec = new TestSparderQueryPlanExec(engine);

        Assert.assertNull(exec.execute(null, null));
        Assert.assertEquals(2, throwExceptionAtFirstTime.callNumber);
        Assert.assertTrue(QueryContext.current().isForceTableIndex());
        Assert.assertTrue(QueryContext.current().getSecondStorageUsageMap().isEmpty());

        //Now QueryContext.current().isForceTableIndex() == true
        ThrowExceptionAtFirstTime throwExceptionAtFirstTime2 = new ThrowExceptionAtFirstTime(true);
        ThrownExceptionEngine engine2 = new ThrownExceptionEngine(throwExceptionAtFirstTime2);
        exec.updateEngine(engine2);

        thrown.expect(SparkException.class);
        try {
            exec.execute(null, null);
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
            exec.execute(null, null);
        } finally {
            Assert.assertEquals(1, throwExceptionAtFirstTime.callNumber);
            Assert.assertFalse(QueryContext.current().isForceTableIndex());
            Assert.assertEquals(0, QueryContext.current().getSecondStorageUsageMap().size());
        }
    }

}
