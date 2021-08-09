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

package io.kyligence.kap.tool;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class KylinConfigCheckCLITest extends NLocalFileMetadataTestCase {
    @Test
    public void testCorrectConfig() throws IOException {
        final File kylinConfDir = KylinConfig.getKylinConfDir();
        File kylin_properties_override = new File(kylinConfDir, "kylin.properties.override");
        IOUtils.copy(new ByteArrayInputStream("kylin.kerberos.platform=FI".getBytes(Charset.defaultCharset())),
                new FileOutputStream(kylin_properties_override));

        PrintStream o = System.out;
        File f = File.createTempFile("check", ".tmp");
        PrintStream tmpOut = new PrintStream(new FileOutputStream(f), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);

        KylinConfigCheckCLI.execute();

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();

        assertEquals("", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
        FileUtils.forceDelete(kylin_properties_override);
        System.setOut(o);
    }

    @Test
    public void testHasErrorConfig() throws IOException {
        final File kylinConfDir = KylinConfig.getKylinConfDir();
        File kylin_properties_override = new File(kylinConfDir, "kylin.properties.override");
        IOUtils.copy(new ByteArrayInputStream("n.kerberos.platform=FI".getBytes(Charset.defaultCharset())),
                new FileOutputStream(kylin_properties_override));

        PrintStream o = System.out;
        File f = File.createTempFile("check", ".tmp");
        PrintStream tmpOut = new PrintStream(new FileOutputStream(f), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);

        KylinConfigCheckCLI.execute();

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();

        assertEquals("n.kerberos.platform", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
        FileUtils.forceDelete(kylin_properties_override);
        System.setOut(o);
    }

    @Before
    public void setUp() throws Exception {
        this.createTestMetadata();
    }

    @After
    public void after() throws Exception {
        this.cleanupTestMetadata();
    }
}