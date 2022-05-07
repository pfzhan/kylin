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

package io.kyligence.kap.parser.loader;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class AddToClassPathActionTest extends NLocalFileMetadataTestCase {

    private static final String JAR_NAME = "custom_parser.jar";

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
    }

    @Test
    public void testRun() {

        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        // ../examples/test_data/21767/metadata
        Path metaPath = new Path(kylinConfig.getMetadataUrl().toString());

        Path jarPath = new Path(String.format("%s/%s/%s", metaPath.getParent().toString(), "jars", JAR_NAME));
        String jarAbsPath = new File(jarPath.toString()).toURI().toString();
        HashSet<String> newPaths = Sets.newHashSet(jarAbsPath);

        {
            AddToClassPathAction action1 = new AddToClassPathAction(Thread.currentThread().getContextClassLoader(),
                    newPaths);
            ParserClassLoader parserClassLoader1 = action1.run();
            Assert.assertNotNull(parserClassLoader1);

            AddToClassPathAction action2 = new AddToClassPathAction(parserClassLoader1, newPaths);
            ParserClassLoader parserClassLoader2 = action2.run();
            Assert.assertNotNull(parserClassLoader2);
        }
        {
            // 3 ParserClassLoader
            ParserClassLoader parserClassLoader3 = new ParserClassLoader(
                    newPaths.stream().map(ClassLoaderUtilities::urlFromPathString).toArray(URL[]::new));
            try {
                parserClassLoader3.close();
            } catch (IOException e) {
                Assert.fail();
            }
        }

        {
            // 4 ParserClassLoader
            ParserClassLoader parserClassLoader4 = new ParserClassLoader(
                    newPaths.stream().map(ClassLoaderUtilities::urlFromPathString).toArray(URL[]::new));
            try {
                parserClassLoader4.close();
                parserClassLoader4.addURL(ClassLoaderUtilities.urlFromPathString(jarAbsPath));
                Assert.fail();
            } catch (Exception e) {
                Assert.assertTrue(e instanceof IllegalStateException);
            }
        }

        // 4 throw exception
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            new AddToClassPathAction(null, newPaths);
        });
    }

}
