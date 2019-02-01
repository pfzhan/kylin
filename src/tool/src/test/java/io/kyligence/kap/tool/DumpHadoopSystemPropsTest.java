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

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Method;
import java.util.TreeMap;

public class DumpHadoopSystemPropsTest {

    @Test
    public void testDumpProps() throws Exception {
        // test diffSystemProps
        Method diffSystemProps = DumpHadoopSystemProps.class.getDeclaredMethod("diffSystemProps", String.class);
        diffSystemProps.setAccessible(true);
        // for case same key and value exists in system props and the props file
        System.setProperty("mapreduce.combine.class", "myclass");
        // for case same key but different value
        System.setProperty("mapred.same.key", "value1");
        // for case property only exists in system props
        System.setProperty("mapreduce.system.unique", "test");

        String propsURL = mockTempPropsFile().getAbsolutePath();
        TreeMap<String, String> propsMap = (TreeMap<String, String>) diffSystemProps.invoke(new DumpHadoopSystemProps(), propsURL);
        Assert.assertEquals(2, propsMap.size());
        Assert.assertEquals("mapred.same.key", propsMap.firstKey());
        Assert.assertEquals("value2", propsMap.firstEntry().getValue());
        Assert.assertEquals("10", propsMap.get("mapred.tip.id"));

        // test diffSystemEnvs
        Method diffSystemEnvs = DumpHadoopSystemProps.class.getDeclaredMethod("diffSystemEnvs", String.class);
        diffSystemEnvs.setAccessible(true);
        String envsURL = mockTempEnvFile().getAbsolutePath();
        TreeMap<String, String> envsMap = (TreeMap<String, String>) diffSystemEnvs.invoke(new DumpHadoopSystemProps(), envsURL);
        Assert.assertEquals(1, envsMap.size());
        Assert.assertEquals("mapred.map.child.env", envsMap.firstKey());
        Assert.assertEquals("myenv", envsMap.firstEntry().getValue());

        // test output() function
        Method output = DumpHadoopSystemProps.class.getDeclaredMethod("output", TreeMap.class, TreeMap.class, File.class);
        output.setAccessible(true);
        File tempFile = File.createTempFile("systemProps", ".tmp");
        output.invoke(new DumpHadoopSystemProps(), propsMap, envsMap, tempFile);
        BufferedReader in = new BufferedReader(new FileReader(tempFile));
        Assert.assertEquals("export mapred.map.child.env=myenv", in.readLine());
        Assert.assertEquals("export kylin_hadoop_opts=\" -Dmapred.same.key=value2  -Dmapred.tip.id=10 \"", in.readLine());
        Assert.assertEquals("rm -f " + tempFile.getAbsolutePath(), in.readLine());

        in.close();
        System.clearProperty("mapreduce.combine.class");
        System.clearProperty("mapred.same.key");
        System.clearProperty("mapreduce.system.unique");
    }

    private File mockTempPropsFile() throws IOException {
        File propsFile = File.createTempFile("test", ".props");
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(propsFile))) {
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
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(envFile))) {
            writer.write("mapred.map.child.env=myenv");
        }
        return envFile;
    }
}
