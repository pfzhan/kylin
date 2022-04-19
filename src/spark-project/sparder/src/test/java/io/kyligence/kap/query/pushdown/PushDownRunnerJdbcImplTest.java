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

package io.kyligence.kap.query.pushdown;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class PushDownRunnerJdbcImplTest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata();
    }

    public static void staticCreateTestMetadata(String... overlay) {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata(Lists.newArrayList(overlay));
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        tempMetadataDirectory = new File(tempMetadataDir);
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            // ignore it
        }
        cleanSingletonInstances();
    }

    private static void cleanSingletonInstances() {
        try {
            getInstances().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getGlobalInstances().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstancesFromSingleton().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstanceByProjectFromSingleton().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstanceByProject().clear();
        } catch (Exception e) {
            //ignore in it
        }
    }

    @After
    public void tearDown() {
        cleanupTestMetadata();
    }

    public void createTestMetadata(String... overlay) {
        staticCreateTestMetadata(overlay);
        val kylinHomePath = new File(getTestConfig().getMetadataUrl().toString()).getParentFile().getAbsolutePath();
        overwriteSystemProp("KYLIN_HOME", kylinHomePath);
        val jobJar = io.kyligence.kap.common.util.FileUtils.findFile(
                new File(kylinHomePath, "../../../assembly/target/").getAbsolutePath(), "kap-assembly(.?)\\.jar");
        getTestConfig().setProperty("kylin.engine.spark.job-jar", jobJar == null ? "" : jobJar.getAbsolutePath());
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        getTestConfig().setProperty("kylin.streaming.enabled", "true");
    }

    @Test
    public void testPushdownJdbc() throws Exception {
        Connection h2Connection = DriverManager.getConnection("jdbc:h2:mem:db_default", "sa", "");
        NProjectManager npr = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = npr.getProject("default");
        projectInstance.setDefaultDatabase("SSB");
        LinkedHashMap<String, String> overrideKylinProps = Maps.newLinkedHashMap();
        overrideKylinProps.put("kylin.query.pushdown.jdbc.url", "jdbc:h2:mem:db_default");
        overrideKylinProps.put("kylin.query.pushdown.jdbc.driver", "org.h2.Driver");
        overrideKylinProps.put("kylin.query.pushdown.jdbc.username", "sa");
        overrideKylinProps.put("kylin.query.pushdown.jdbc.password", "");
        projectInstance.setOverrideKylinProps(overrideKylinProps);
        npr.updateProject(projectInstance);
        KylinConfigExt config = projectInstance.getConfig();
        PushDownRunnerJdbcImpl pushDownRunnerJdbc = new PushDownRunnerJdbcImpl();
        pushDownRunnerJdbc.init(config);
        String sql = "select 1";
        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();
        pushDownRunnerJdbc.executeQuery(sql, returnRows, returnColumnMeta, "default");
        Assert.assertEquals("1", returnRows.get(0).get(0));
    }
}
