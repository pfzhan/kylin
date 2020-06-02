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

package io.kyligence.kap.smart.query;

import java.util.Collection;
import java.util.concurrent.ConcurrentNavigableMap;

import org.apache.kylin.query.relnode.OLAPContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.smart.query.mockup.MockupQueryExecutor;

public class DefaultQueryRunnerTest extends NLocalFileMetadataTestCase {

    private static final String DEFAULT_PROJECT = "default";

    @Before
    public void setup() {
        createTestMetadata();
    }

    @After
    public void tearDown() {
        staticCleanupTestMetadata();
    }

    @Test
    public void testExecute() throws Exception {
        String[] sqls = new String[] { "SELECT SUM(PRICE * ITEM_COUNT), CAL_DT FROM TEST_KYLIN_FACT GROUP BY CAL_DT",
                "SELECT PRICE, ITEM_COUNT, CAL_DT FROM TEST_KYLIN_FACT" };
        AbstractQueryRunner queryRunner1 = NQueryRunnerFactory.createForModelSuggestion(getTestConfig(),
                DEFAULT_PROJECT, sqls, 1);
        queryRunner1.execute();
        ConcurrentNavigableMap<Integer, Collection<OLAPContext>> olapContexts = queryRunner1.getOlapContexts();
        Assert.assertEquals(2, olapContexts.size());

        Assert.assertEquals(1, olapContexts.get(0).size());
        OLAPContext olapContext1 = olapContexts.get(0).iterator().next();
        Assert.assertNull(olapContext1.getTopNode());
        Assert.assertNull(olapContext1.getParentOfTopNode());
        Assert.assertEquals(0, olapContext1.allOlapJoins.size());

        Assert.assertEquals(1, olapContexts.get(1).size());
        OLAPContext olapContext2 = olapContexts.get(1).iterator().next();
        Assert.assertNull(olapContext2.getTopNode());
        Assert.assertNull(olapContext2.getParentOfTopNode());
        Assert.assertEquals(0, olapContext2.allOlapJoins.size());

        AbstractQueryRunner queryRunner2 = NQueryRunnerFactory.createForModelSuggestion(getTestConfig(),
                DEFAULT_PROJECT, sqls, 1);
        queryRunner2.execute(new MockupQueryExecutor());
        Assert.assertEquals(0, queryRunner2.getOlapContexts().size());
    }
}
