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

package io.kyligence.kap.query;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.query.ITKylinQueryTest;
import org.apache.kylin.query.routing.Candidate;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.cube.model.NDataflow;

public class NKapQueryTest extends NKylinTestBase {
    private static final Logger logger = LoggerFactory.getLogger(ITKylinQueryTest.class);

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @BeforeClass
    public static void setUp() throws Exception {
        logger.info("setUp in NKapQueryTest");
        Map<String, Integer> priorities = Maps.newHashMap();
        priorities.put(NDataflow.REALIZATION_TYPE, 0);
        Candidate.setPriorities(priorities);

        joinType = "left";

        NKylinTestBase.setupAll();
        KylinConfig.getInstanceFromEnv().setProperty("kap.storage.columnar.hdfs-dir",
                NLocalFileMetadataTestCase.tempMetadataDirectory.getAbsolutePath() + "/parquet/");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        logger.info("tearDown in NKapQueryTest");
        Candidate.restorePriorities();

        NKylinTestBase.clean();
    }

    @Test
    public void testTempQuery() throws Exception {
        try {
            PRINT_RESULT = true;
            batchExecuteQuery(getQueryFolderPrefix() + "src/test/resources/nquery/temp");
        } finally {
            PRINT_RESULT = false;
        }
    }
}
