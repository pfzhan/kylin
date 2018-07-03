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

package org.apache.kylin.metadata.model;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Map;

import static org.junit.Assert.assertTrue;

public class NJoinsTreeTest extends NLocalFileMetadataTestCase {
    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }

    @Test
    public void testBasics() {
        NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = mgr.getDataModelDesc("nmodel_basic");
        JoinsTree joinsTree = model.getJoinsTree();

        JoinsTree.Chain chain = joinsTree.getTableChains().get("BUYER_COUNTRY");
        assertTrue(chain.table == model.findTable("BUYER_COUNTRY"));
        assertTrue(chain.fkSide.table == model.findTable("BUYER_ACCOUNT"));
        assertTrue(chain.fkSide.fkSide.table == model.findTable("TEST_ORDER"));
        assertTrue(chain.fkSide.fkSide.fkSide.table == model.findTable("TEST_KYLIN_FACT"));
        assertTrue(chain.fkSide.fkSide.fkSide.join == null);
        assertTrue(chain.fkSide.fkSide.fkSide.fkSide == null);
    }

    @Test
    public void testMatch() {
        NDataModelManager mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), "default");
        NDataModel model = mgr.getDataModelDesc("nmodel_basic");
        JoinsTree joinsTree = model.getJoinsTree();

        Map<String, String> matchUp = joinsTree.matches(joinsTree);
        for (Map.Entry<String, String> e : matchUp.entrySet()) {
            assertTrue(e.getKey().equals(e.getValue()));
        }
        assertTrue(model.getAllTables().size() == matchUp.size());
    }
}
