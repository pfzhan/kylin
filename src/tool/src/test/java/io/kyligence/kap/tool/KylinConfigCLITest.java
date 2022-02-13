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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.Charset;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.EncryptUtil;
import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;

public class KylinConfigCLITest extends NLocalFileMetadataTestCase {
    @Test
    public void testGetProperty() throws IOException {
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        PrintStream tmpOut = new PrintStream(new FileOutputStream(f), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);
        KylinConfigCLI.execute(new String[] { "kylin.env" });

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        assertEquals("UT", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
        System.setOut(o);
    }

    @Test
    public void testGetEncryptProperty() throws IOException {
        final KylinConfig config = getTestConfig();
        String property = "kylin.influxdb.password";
        config.setProperty(property, "ENC('YeqVr9MakSFbgxEec9sBwg==')");
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        PrintStream tmpOut = new PrintStream(new FileOutputStream(f), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);
        KylinConfigCLI.execute(new String[] { property, EncryptUtil.DEC_FLAG });

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        assertEquals("kylin", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
        System.setOut(o);
    }

    @Test
    public void testGetUnEncryptPropertyWithDECFlag() throws IOException {
        final KylinConfig config = getTestConfig();
        String property = "kylin.influxdb.password";
        config.setProperty(property, "kylin");
        PrintStream o = System.out;
        File f = File.createTempFile("cfg", ".tmp");
        PrintStream tmpOut = new PrintStream(new FileOutputStream(f), false, Charset.defaultCharset().name());
        System.setOut(tmpOut);
        KylinConfigCLI.execute(new String[] { property, EncryptUtil.DEC_FLAG });

        String val = FileUtils.readFileToString(f, Charset.defaultCharset()).trim();
        assertEquals("kylin", val);
        tmpOut.close();
        FileUtils.forceDelete(f);
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