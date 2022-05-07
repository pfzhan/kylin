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

import org.junit.Assert;
import org.junit.Test;

public class JarInfoTest {

    private String project = "streaming_test";
    private String jarName = "test.jar";
    private String jarPath = "/test/test.jar";

    @Test
    public void testCreate() {
        {
            JarInfo jarInfo = new JarInfo(project, jarName, jarPath, STREAMING_CUSTOM_PARSER);
            Assert.assertEquals(project, jarInfo.getProject());
            Assert.assertEquals(jarName, jarInfo.getJarName());
            Assert.assertEquals(jarPath, jarInfo.getJarPath());
            Assert.assertEquals(STREAMING_CUSTOM_PARSER, jarInfo.getJarType());
            Assert.assertEquals("/streaming_test/jar/" + STREAMING_CUSTOM_PARSER + "_" + jarName + ".json",
                    jarInfo.getResourcePath());
        }

        {
            JarInfo jarInfo = new JarInfo();
            Assert.assertNull(jarInfo.getProject());
            Assert.assertNull(jarInfo.getJarName());
            Assert.assertNull(jarInfo.getJarPath());
            Assert.assertNull(jarInfo.getJarType());
            Assert.assertNull(jarInfo.resourceName());
        }
    }

}
