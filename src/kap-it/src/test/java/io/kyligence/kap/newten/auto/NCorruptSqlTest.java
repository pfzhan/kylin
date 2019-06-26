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

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.smart.NSmartMaster;
import io.kyligence.kap.smart.common.AccelerateInfo;

public class NCorruptSqlTest extends NAutoTestBase {

    /**
     * DDL: not supported sql.
     */
    @Test
    public void testDDL() throws IOException {
        final NSmartMaster smartMaster = proposeWithSmartMaster("newten", new TestScenario("ddl-sql"));
        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        accelerateInfoMap.forEach((key, value) -> {
            final String blockMessage = value.getFailedCause().getMessage();
            Assert.assertTrue(blockMessage.contains("Not Supported SQL"));
        });
    }

    /**
     * Simple SQL: this case will not propose layouts, and blocking cause is also null.
     */
    @Test
    public void testSimpleQuery() throws IOException {
        final NSmartMaster smartMaster = proposeWithSmartMaster("newten", new TestScenario("simple-sql"));
        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        accelerateInfoMap.forEach((key, value) -> {
            Assert.assertNull(value.getFailedCause());
            Assert.assertEquals(0, value.getRelatedLayouts().size());
        });
    }

    /**
     * Invalid SQL: this case will lead to parsing error.
     */
    @Test
    public void testInvalidQuery() throws IOException {
        final NSmartMaster smartMaster = proposeWithSmartMaster("newten", new TestScenario("parse-error"));
        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        accelerateInfoMap.forEach((key, value) -> Assert.assertTrue(value.isFailed() || value.isPending()));
    }

    /**
     * Other cases: in this case sql parsing is ok, but will lead to NPE, such as #6548, #7504
     */
    @Test
    public void testOtherCases() throws IOException {
        final NSmartMaster smartMaster = proposeWithSmartMaster("newten", new TestScenario("other_cases"));
        final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
        accelerateInfoMap.forEach((key, value) -> {
            Assert.assertTrue(value.isPending() || value.isFailed());
        });
    }

    @Test
    public void testIssueRelatedSqlEndsNormally() {

        KylinConfig.getInstanceFromEnv().setProperty("kylin.query.calcite.extras-props.conformance", "LENIENT");
        try {
            final NSmartMaster smartMaster = proposeWithSmartMaster("newten", new TestScenario("issues-related-sql"));
            final Map<String, AccelerateInfo> accelerateInfoMap = smartMaster.getContext().getAccelerateInfoMap();
            accelerateInfoMap.forEach((key, value) -> Assert.assertFalse(value.isFailed()));
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }

    }

    private static final String IT_SQL_KAP_DIR = "../kap-it/src/test/resources/corrupt-query";

    @Override
    String getFolder(String subFolder) {
        return IT_SQL_KAP_DIR + File.separator + subFolder;
    }
}
