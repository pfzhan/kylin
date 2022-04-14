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

package io.kyligence.kap.rest.source;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.test.util.ReflectionTestUtils;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.service.SourceTestCase;

public class DataSourceStateTest extends SourceTestCase {

    private static final String PROJECT = "default";
    private static final String DATABASE = "SSB";

    @Before
    public void setUp() {
        super.setup();
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    @Test
    public void testLoadAllSourceInfoToCacheForcedTrue() {
        DataSourceState instance = DataSourceState.getInstance();

        instance.loadAllSourceInfoToCacheForced(PROJECT, true);
        List<String> cacheTables = instance.getTables(PROJECT, DATABASE);
        Assert.assertFalse(cacheTables.isEmpty());
    }

    @Test
    public void testLoadAllSourceInfoToCacheForcedFalse() {
        DataSourceState instance = DataSourceState.getInstance();

        instance.loadAllSourceInfoToCacheForced(PROJECT, false);
        List<String> cacheTables = instance.getTables(PROJECT, DATABASE);
        Assert.assertTrue(cacheTables.isEmpty());
    }

    @Test
    public void testLoadAllSourceInfoToCache() {
        DataSourceState instance = DataSourceState.getInstance();
        Thread thread = new Thread(instance);
        getTestConfig().setProperty("kylin.kerberos.project-level-enabled", "true");
        getTestConfig().setProperty("kylin.source.load-hive-table-wait-sparder-seconds", "5");
        getTestConfig().setProperty("kylin.source.load-hive-table-wait-sparder-interval-seconds", "1");
        thread.start();
        try {
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
                List<String> cacheTables = instance.getTables(PROJECT, DATABASE);
                return !cacheTables.isEmpty();
            });
        } catch (Exception e) {
            // ignore
        }
        Assert.assertTrue(instance.getTables(PROJECT, DATABASE).isEmpty());
        getTestConfig().setProperty("kylin.source.load-hive-table-wait-sparder-seconds", "900");
        getTestConfig().setProperty("kylin.source.load-hive-table-wait-sparder-interval-seconds", "10");
        Thread thread2 = new Thread(instance);
        thread2.start();
        try {
            Awaitility.await().atMost(2, TimeUnit.SECONDS).until(() -> {
                List<String> cacheTables = instance.getTables(PROJECT, DATABASE);
                return !cacheTables.isEmpty();
            });
        } catch (Exception e) {
            // ignore
        }
        thread2.interrupt();
        getTestConfig().setProperty("kylin.kerberos.project-level-enabled", "false");
        instance.run();
        List<String> cacheTables1 = instance.getTables(PROJECT, DATABASE);
        Assert.assertFalse(cacheTables1.isEmpty());

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.source.load-hive-tablename-enable", "false");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
        projectInstance.setPrincipal("test");
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());
        instance.run();
        List<String> cacheTables2 = instance.getTables(PROJECT, DATABASE);
        Assert.assertFalse(cacheTables2.isEmpty());
    }

    @Test
    public void testGetTablesJdbc() {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(PROJECT);
        LinkedHashMap<String, String> overrideKylinProps = projectInstance.getOverrideKylinProps();
        overrideKylinProps.put("kylin.query.force-limit", "-1");
        overrideKylinProps.put("kylin.source.default", "8");
        ProjectInstance projectInstanceUpdate = ProjectInstance.create(projectInstance.getName(),
                projectInstance.getOwner(), projectInstance.getDescription(), overrideKylinProps);
        projectManager.updateProject(projectInstance, projectInstanceUpdate.getName(),
                projectInstanceUpdate.getDescription(), projectInstanceUpdate.getOverrideKylinProps());

        DataSourceState instance = DataSourceState.getInstance();
        NHiveSourceInfo sourceInfo = new NHiveSourceInfo();
        Map<String, List<String>> testData = new HashMap<>();
        testData.put("t", Arrays.asList("aa", "ab", "bc"));
        sourceInfo.setTables(testData);
        instance.putCache("project#default", sourceInfo);
        Assert.assertFalse(instance.getTables(PROJECT, "t").isEmpty());
    }

    @Test
    public void testCheckIsAllNode() {
        KylinConfig config = getTestConfig();
        config.setProperty("kylin.source.load-hive-tablename-enabled", "false");
        Assert.assertThrows(KylinException.class,
                () -> ReflectionTestUtils.invokeMethod(DataSourceState.getInstance(), "checkIsAllNode"));
        config.setProperty("kylin.source.load-hive-tablename-enabled", "true");
    }

}
