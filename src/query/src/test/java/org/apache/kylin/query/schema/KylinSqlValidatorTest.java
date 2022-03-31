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

package org.apache.kylin.query.schema;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.realization.RealizationStatusEnum;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.cube.model.IndexPlan;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.cube.model.NIndexPlanManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.query.engine.QueryExec;
import lombok.val;

public class KylinSqlValidatorTest extends NLocalFileMetadataTestCase {

    private final String PROJECT = "tpch";

    @Before
    public void setup() throws IOException {
        createTestMetadata();
        val mgr = NDataModelManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT);
        val serializer = mgr.getDataModelSerializer();
        val contents = StringUtils.join(Files.readAllLines(
                new File("src/test/resources/ut_meta/validator/model.json").toPath(), Charset.defaultCharset()), "\n");
        val bais = IOUtils.toInputStream(contents, Charset.defaultCharset());
        val deserialized = serializer.deserialize(new DataInputStream(bais));
        deserialized.setProject(PROJECT);
        val model = mgr.createDataModelDesc(deserialized, "ADMIN");

        val emptyIndex = new IndexPlan();
        emptyIndex.setUuid(model.getUuid());
        NIndexPlanManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).createIndexPlan(emptyIndex);

        val df = NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).createDataflow(emptyIndex,
                model.getOwner());
        NDataflowManager.getInstance(KylinConfig.getInstanceFromEnv(), PROJECT).updateDataflowStatus(df.getId(),
                RealizationStatusEnum.ONLINE);

    }

    @After
    public void teardown() {
        this.cleanupTestMetadata();
    }

    private void assertExpandFields(String sql, int expectedFiledNum) {
        QueryExec queryExec = new QueryExec(PROJECT, KylinConfig.getInstanceFromEnv());
        RelNode rel = queryExec.wrapSqlTest(exec -> {
            try {
                return exec.parseAndOptimize(sql);
            } catch (SqlParseException e) {
                Assert.fail(e.toString());
                return null;
            }
        });
        Assert.assertEquals(expectedFiledNum, rel.getRowType().getFieldCount());
    }

    @Test
    public void testExpandSelectStar() {
        String[] sqls = new String[] { "select * from tpch.nation", "select t1.* from tpch.nation t1", };
        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "TRUE");
        for (String sql : sqls) {
            assertExpandFields(sql, 5);
        }

        overwriteSystemProp("kylin.query.metadata.expose-computed-column", "FALSE");
        for (String sql : sqls) {
            assertExpandFields(sql, 4);
        }
    }

}
