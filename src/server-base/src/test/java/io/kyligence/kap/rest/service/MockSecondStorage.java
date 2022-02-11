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

package io.kyligence.kap.rest.service;

import io.kyligence.kap.clickhouse.ClickHouseStorage;
import io.kyligence.kap.clickhouse.management.ClickHouseConfigLoader;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.secondstorage.SecondStorage;
import io.kyligence.kap.secondstorage.SecondStorageUtil;
import io.kyligence.kap.secondstorage.config.Cluster;
import io.kyligence.kap.secondstorage.config.Node;
import lombok.val;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import static io.kyligence.kap.secondstorage.SecondStorageConstants.CONFIG_SECOND_STORAGE_CLUSTER;

public class MockSecondStorage {
    public static void mock(String project, List<Node> nodes, NLocalFileMetadataTestCase testCase) throws IOException {
        testCase.overwriteSystemProp("kylin.second-storage.class", ClickHouseStorage.class.getCanonicalName());
        Cluster cluster = new Cluster();
        cluster.setKeepAliveTimeout("600000");
        cluster.setSocketTimeout("600000");
        cluster.setNodes(nodes);
        File file = File.createTempFile("clickhouse", ".yaml");
        ClickHouseConfigLoader.getConfigYaml().dump(JsonUtil.readValue(JsonUtil.writeValueAsString(cluster),
                Map.class), new PrintWriter(file, Charset.defaultCharset().name()));
        Unsafe.setProperty(CONFIG_SECOND_STORAGE_CLUSTER, file.getAbsolutePath());
        SecondStorage.init(true);
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            val nodeGroupManager = SecondStorageUtil.nodeGroupManager(testCase.getTestConfig(), project);
            Assert.assertTrue(nodeGroupManager.isPresent());
            return nodeGroupManager.get().makeSureRootEntity("");
        }, project, 1, UnitOfWork.DEFAULT_EPOCH_ID);
    }

}
