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
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import com.google.common.collect.Sets;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class ParserClassLoaderStateTest extends NLocalFileMetadataTestCase {

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private static final String PROJECT = "streaming_test";
    private static final String JAR_NAME = "custom_parser.jar";
    private static String JAR_ABS_PATH;

    @Before
    public void setup() throws Exception {
        createTestMetadata();
        init();
    }

    @After
    public void teardown() {
        cleanupTestMetadata();
        ParserClassLoaderState.getInstance(PROJECT).setLoadedJars(Sets.newCopyOnWriteArraySet());
    }

    public void init() {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        Path metaPath = new Path(kylinConfig.getMetadataUrl().toString());
        Path jarPath = new Path(String.format("%s/%s/%s", metaPath.getParent().toString(), "jars", JAR_NAME));
        JAR_ABS_PATH = new File(jarPath.toString()).toURI().toString();
    }

    @Test
    public void testRegisterAndUnRegisterJar() {

        HashSet<String> newPaths = Sets.newHashSet(JAR_ABS_PATH);
        ParserClassLoaderState instance = ParserClassLoaderState.getInstance(PROJECT);

        // 1 normal
        instance.registerJars(newPaths);

        // 2 register double jar exception
        try {
            instance.registerJars(newPaths);
        } catch (Exception e) {
            Assert.assertTrue(e instanceof IllegalArgumentException);
        }
        instance.unregisterJar(newPaths);

        // 3 exception
        thrown.expect(IllegalArgumentException.class);
        instance.setLoadedJars(null);
        instance.unregisterJar(Sets.newHashSet("test"));
    }
}
