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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.Locale;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class RenameUserResourceToolTest extends NLocalFileMetadataTestCase {

    private KylinConfig config;

    @Before
    public void setup() throws IOException {
        createTestMetadata("src/test/resources/ut_upgrade_tool");
        config = KylinConfig.getInstanceFromEnv();
    }

    @After
    public void after() {
        cleanupTestMetadata();
    }

    /**
     * rename_user_1 -> rename_user_11
     * <p>
     * rename_user_1 is rename_project_1's project management
     * rename_user_1 is rename_project_2's project owner model owner
     */
    @Test
    public void testRenameUser() {
        String data = "y\r\n";
        InputStream stdin = System.in;
        try {
            System.setIn(new ByteArrayInputStream(data.getBytes(Charset.defaultCharset())));
            val renameResourceTool = new RenameUserResourceTool();
            renameResourceTool.execute(new String[] { "-dir", config.getMetadataUrl().toString(), "--user",
                    "rename_user_1", "--collect-only", "false" });

            config.clearManagers();
            ResourceStore.clearCache();

            val projectManager = NProjectManager.getInstance(config);

            // project owner
            val project2 = projectManager.getProject("rename_project_2");
            Assert.assertEquals("rename_user_11", project2.getOwner());

            // model owner
            val dataModelManager = NDataModelManager.getInstance(config, "rename_project_2");
            val dataModel = dataModelManager.getDataModelDescByAlias("rename_model_2");
            Assert.assertEquals("rename_user_11", dataModel.getOwner());

            config.clearManagers();
            ResourceStore.clearCache();
            ResourceStore resourceStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());

            // project acl
            String projectAclPath = String.format(Locale.ROOT, "/_global/acl/%s", project2.getUuid());
            RawResource rs = resourceStore.getResource(projectAclPath);
            boolean renamed = false;
            try (InputStream is = rs.getByteSource().openStream()) {
                JsonNode aclJsonNode = JsonUtil.readValue(is, JsonNode.class);

                if (aclJsonNode.has("entries")) {
                    ArrayNode entries = (ArrayNode) aclJsonNode.get("entries");
                    for (JsonNode entry : entries) {
                        // p for person
                        if (entry.has("p")) {
                            String p = entry.get("p").asText();
                            if (StringUtils.equals(p, "rename_user_11")) {
                                renamed = true;
                                Assert.assertEquals(16, entry.get("m").intValue());
                            }
                        }
                    }
                }

            } catch (IOException e) {
            }

            Assert.assertTrue(renamed);

            renamed = false;
            val project1 = projectManager.getProject("rename_project_1");
            projectAclPath = String.format(Locale.ROOT, "/_global/acl/%s", project1.getUuid());
            rs = resourceStore.getResource(projectAclPath);
            try (InputStream is = rs.getByteSource().openStream()) {
                JsonNode aclJsonNode = JsonUtil.readValue(is, JsonNode.class);

                if (aclJsonNode.has("entries")) {
                    ArrayNode entries = (ArrayNode) aclJsonNode.get("entries");
                    for (JsonNode entry : entries) {
                        // p for person
                        if (entry.has("p")) {
                            String p = entry.get("p").asText();
                            if (StringUtils.equals(p, "rename_user_11")) {
                                renamed = true;
                                Assert.assertEquals(32, entry.get("m").intValue());
                            }
                        }
                    }
                }

            } catch (IOException e) {
            }
            Assert.assertTrue(renamed);

            config.clearManagers();
            ResourceStore.clearCache();
            config.clearManagersByProject("rename_project_1");

            // acl
            AclTCRManager tcrManager = AclTCRManager.getInstance(config, "rename_project_1");
            Assert.assertNotNull(tcrManager.getAclTCR("rename_user_11", true));

            config.clearManagers();
            ResourceStore.clearCache();
            config.clearManagersByProject("rename_project_2");

            tcrManager = AclTCRManager.getInstance(config, "rename_project_2");
            Assert.assertNotNull(tcrManager.getAclTCR("rename_user_11", true));

        } finally {
            System.setIn(stdin);
        }
    }
}