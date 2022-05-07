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

package io.kyligence.kap.metadata.jar;

import static io.kyligence.kap.metadata.jar.JarTypeEnum.STREAMING_CUSTOM_PARSER;

import java.util.List;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class JarInfoManagerTest extends NLocalFileMetadataTestCase {

    private JarInfoManager manager;
    private static final String project = "streaming_test";
    private static final String jarName = "custom_parser_test.jar";
    private static final String jarPath = "/streaming_test/jar/custom_parser_test.jar";
    private static final String jarTypeName = "STREAMING_CUSTOM_PARSER_custom_parser_test.jar";
    private static final String test = "test";

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
        manager = JarInfoManager.getInstance(getTestConfig(), project);
    }

    @After
    public void tearDown() {
        this.cleanupTestMetadata();
    }

    @Test
    public void testGetJarInfo() {

        JarInfo jarInfo = manager.getJarInfo(jarTypeName);
        Assert.assertNotNull(jarInfo);
        Assert.assertEquals(project, jarInfo.getProject());
        Assert.assertEquals(jarName, jarInfo.getJarName());
        Assert.assertEquals(jarPath, jarInfo.getJarPath());
        Assert.assertEquals(jarTypeName, jarInfo.resourceName());

        JarInfo jarInfo2 = manager.getJarInfo(null);
        Assert.assertNull(jarInfo2);
    }

    @Test
    public void testCreateJarInfoNull() {
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createJarInfo(null));
    }

    @Test
    public void testCreateJarInfoNullResourceName() {
        JarInfo jarInfo = new JarInfo();
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createJarInfo(jarInfo));
    }

    @Test
    public void testCreateJarInfoContains() {
        JarInfo jarInfo = new JarInfo(project, jarName, jarPath, STREAMING_CUSTOM_PARSER);
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.createJarInfo(jarInfo));
    }

    @Test
    public void testCreateJarInfo() {
        JarInfo jarInfo = manager.createJarInfo(new JarInfo(project, test, jarPath, STREAMING_CUSTOM_PARSER));
        Assert.assertNotNull(jarInfo);
        manager.removeJarInfo(jarInfo.resourceName());
    }

    @Test
    public void testUpdateJarInfoNotContains() {
        JarInfo jarInfo = new JarInfo(project, test + "1", jarPath, STREAMING_CUSTOM_PARSER);
        Assert.assertThrows(IllegalArgumentException.class, () -> manager.updateJarInfo(jarInfo));
    }

    @Test
    public void testUpdateJarInfo() {
        JarInfo jarInfo1 = manager.createJarInfo(new JarInfo(project, jarName + "1", jarPath, STREAMING_CUSTOM_PARSER));
        jarInfo1.setJarPath(test);
        JarInfo jarInfo2 = manager.updateJarInfo(jarInfo1);
        manager.removeJarInfo(jarInfo1.resourceName());
        Assert.assertEquals(test, jarInfo2.getJarPath());
    }

    @Test
    public void testRemoveJarInfo() {
        {
            JarInfo jarInfo = manager.removeJarInfo(null);
            Assert.assertNull(jarInfo);
        }

        {
            JarInfo jarInfo = manager
                    .createJarInfo(new JarInfo(project, jarName + "1", jarPath, STREAMING_CUSTOM_PARSER));
            Assert.assertNotNull(jarInfo);
        }
    }

    @Test
    public void testListJarInfo() {
        {
            List<JarInfo> jarInfos = manager.listJarInfo();
            Assert.assertFalse(jarInfos.isEmpty());
        }

        {
            List<JarInfo> jarInfos = manager.listJarInfoByType(STREAMING_CUSTOM_PARSER);
            Assert.assertFalse(jarInfos.isEmpty());
        }
    }

}
