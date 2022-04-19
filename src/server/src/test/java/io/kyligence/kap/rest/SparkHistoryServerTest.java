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
package io.kyligence.kap.rest;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import org.apache.spark.deploy.history.HistoryServer;
import org.junit.Before;
import org.junit.Test;

public class SparkHistoryServerTest extends NLocalFileMetadataTestCase {
    @Before
    public void setup() {
        createTestMetadata();
    }

    @Test
    public void testCreateHistoryServer() {
        String originLogDir = getTestConfig().getSparkConfigOverride().get("spark.eventLog.dir");
        getTestConfig().setProperty("kylin.engine.spark-conf.spark.eventLog.dir", originLogDir + "/new");
        SparkHistoryServer configuration = new SparkHistoryServer();
        HistoryServer historyServer = configuration.createHistoryServer();
        assert historyServer.boundPort() > 0;
    }
}
