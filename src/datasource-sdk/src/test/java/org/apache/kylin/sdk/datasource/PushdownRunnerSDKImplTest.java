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
package org.apache.kylin.sdk.datasource;

import java.util.List;

import org.apache.kylin.common.QueryContext;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.metadata.querymeta.SelectedColumnMeta;
import org.apache.kylin.sdk.datasource.framework.JdbcConnectorTest;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.Lists;

import io.kyligence.kap.metadata.project.NProjectManager;

public class PushdownRunnerSDKImplTest extends JdbcConnectorTest {
    @Test
    public void testExecuteQuery() throws Exception {
        NProjectManager npr = NProjectManager.getInstance(getTestConfig());
        ProjectInstance projectInstance = npr.getProject("default");
        projectInstance.setDefaultDatabase("SSB");
        npr.updateProject(projectInstance);

        PushDownRunnerSDKImpl pushDownRunnerSDK = new PushDownRunnerSDKImpl();
        pushDownRunnerSDK.init(getTestConfig());
        List<List<String>> returnRows = Lists.newArrayList();
        List<SelectedColumnMeta> returnColumnMeta = Lists.newArrayList();
        String sql = "select count(*) from LINEORDER";
        pushDownRunnerSDK.executeQuery(sql, returnRows, returnColumnMeta, "default");
        Assert.assertEquals("6005", returnRows.get(0).get(0));
    }

    @Test
    public void testExecuteUpdate() throws Exception {
        PushDownRunnerSDKImpl pushDownRunnerSDK = new PushDownRunnerSDKImpl();
        pushDownRunnerSDK.init(getTestConfig());
        String sql = "update SSB.LINEORDER set LO_TAX=1 where LO_ORDERKEY = 1";
        pushDownRunnerSDK.executeUpdate(sql, null);
    }

    @Test
    public void testGetName() {
        PushDownRunnerSDKImpl pushDownRunnerSDK = new PushDownRunnerSDKImpl();
        Assert.assertEquals(QueryContext.PUSHDOWN_RDBMS, pushDownRunnerSDK.getName());
    }
}
