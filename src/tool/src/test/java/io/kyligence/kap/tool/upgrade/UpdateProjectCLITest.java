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

package io.kyligence.kap.tool.upgrade;

import java.util.LinkedHashMap;

import org.apache.kylin.metadata.project.ProjectInstance;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.project.NProjectManager;

public class UpdateProjectCLITest extends NLocalFileMetadataTestCase {

    @Before
    public void setup() {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void updateProject() {
        NProjectManager projectManager = NProjectManager.getInstance(getTestConfig());

        ProjectInstance project = projectManager.getProject("default");

        projectManager.updateProject("default", update_project -> {
            LinkedHashMap<String, String> overrideKylinProps = update_project.getOverrideKylinProps();
            overrideKylinProps.put("kap.metadata.semi-automatic-mode", "true");
            overrideKylinProps.put("kap.query.metadata.expose-computed-column", "false");
        });

        Assert.assertNull(project.getDefaultDatabase());

        UpdateProjectCLI updateProjectCLI = new UpdateProjectCLI();
        updateProjectCLI.execute(new String[] { "-dir", getTestConfig().getMetadataUrl().toString() });

        getTestConfig().clearManagers();
        getTestConfig().clearManagersByProject("default");

        projectManager = NProjectManager.getInstance(getTestConfig());
        project = projectManager.getProject("default");

        LinkedHashMap<String, String> overrideKylinProps = project.getOverrideKylinProps();
        Assert.assertEquals("true", overrideKylinProps.get("kylin.metadata.semi-automatic-mode"));
        Assert.assertEquals("false", overrideKylinProps.get("kylin.query.metadata.expose-computed-column"));
    }
}