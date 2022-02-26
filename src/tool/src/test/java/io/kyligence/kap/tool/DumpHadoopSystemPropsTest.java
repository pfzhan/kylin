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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.TreeMap;

import org.apache.kylin.common.AbstractTestCase;
import org.junit.Assert;
import org.junit.Test;

import io.kyligence.kap.common.util.Unsafe;

public class DumpHadoopSystemPropsTest extends AbstractTestCase {

    @Test
    public void testDumpProps() throws Exception {
        // test diffSystemProps
        Method diffSystemProps = DumpHadoopSystemProps.class.getDeclaredMethod("diffSystemProps", String.class);
        Unsafe.changeAccessibleObject(diffSystemProps, true);
        // for case same key and value exists in system props and the props file
        overwriteSystemProp("mapreduce.combine.class", "myclass");
        // for case same key but different value
        overwriteSystemProp("mapred.same.key", "value1");
        // for case property only exists in system props
        overwriteSystemProp("mapreduce.system.unique", "test");

        String propsURL = mockTempPropsFile().getAbsolutePath();
        TreeMap<String, String> propsMap = (TreeMap<String, String>) diffSystemProps.invoke(new DumpHadoopSystemProps(),
                propsURL);
        Assert.assertEquals(2, propsMap.size());
        Assert.assertEquals("mapred.same.key", propsMap.firstKey());
        Assert.assertEquals("value2", propsMap.firstEntry().getValue());
        Assert.assertEquals("10", propsMap.get("mapred.tip.id"));

        // test diffSystemEnvs
        Method diffSystemEnvs = DumpHadoopSystemProps.class.getDeclaredMethod("diffSystemEnvs", String.class);
        Unsafe.changeAccessibleObject(diffSystemEnvs, true);
        String envsURL = mockTempEnvFile().getAbsolutePath();
        TreeMap<String, String> envsMap = (TreeMap<String, String>) diffSystemEnvs.invoke(new DumpHadoopSystemProps(),
                envsURL);
        Assert.assertEquals(1, envsMap.size());
        Assert.assertEquals("mapred.map.child.env", envsMap.firstKey());
        Assert.assertEquals("myenv", envsMap.firstEntry().getValue());

        // test output() function
        Method output = DumpHadoopSystemProps.class.getDeclaredMethod("output", TreeMap.class, TreeMap.class,
                File.class);
        Unsafe.changeAccessibleObject(output, true);
        File tempFile = File.createTempFile("systemProps", ".tmp");
        try (InputStream in = new FileInputStream(tempFile);
                BufferedReader br = new BufferedReader(new InputStreamReader(in, Charset.defaultCharset()))) {
            output.invoke(new DumpHadoopSystemProps(), propsMap, envsMap, tempFile);
            Assert.assertEquals("export mapred.map.child.env=myenv", br.readLine());
            Assert.assertEquals("export kylin_hadoop_opts=\" -Dmapred.same.key=value2  -Dmapred.tip.id=10 \"",
                    br.readLine());
            Assert.assertEquals("rm -f " + tempFile.getAbsolutePath(), br.readLine());
        }
    }

    private File mockTempPropsFile() throws IOException {
        File propsFile = File.createTempFile("test", ".props");
        try (OutputStream os = new FileOutputStream(propsFile);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            writer.write("mapreduce.combine.class=myclass");
            writer.newLine();
            writer.write("mapred.same.key=value2");
            writer.newLine();
            // property only exists in props file will be exported
            writer.write("mapred.tip.id=10");
        }
        return propsFile;
    }

    private File mockTempEnvFile() throws IOException {
        File envFile = File.createTempFile("test", ".envs");
        try (OutputStream os = new FileOutputStream(envFile);
                BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            writer.write("mapred.map.child.env=myenv");
        }
        return envFile;
    }
}
