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

package io.kyligence.kap.metadata.hdfs;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.engine.mr.HadoopUtil;
import org.apache.kylin.query.KylinTestBase;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import io.kyligence.kap.engine.mr.HDFSResourceStore;

public class ITHDFSResourceStoreTest extends KylinTestBase {
    private static String[] testStrings = new String[] { "Apple", "Banana", "Kylin" };
    private static final String testPath = "/tmp/test";
    private static final String testPath2 = "/tmp/test2";

    @BeforeClass
    public static void setup() throws Exception {
        setupAll();
    }

    @Before
    public void beforeTest() throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        fs.delete(HDFSResourceStore.hdfsPath(testPath, config), true);
        fs.delete(HDFSResourceStore.hdfsPath(testPath2, config), true);
        FSDataOutputStream outputStream = fs.create(HDFSResourceStore.hdfsPath(testPath, config));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(outputStream));
        for (String s : testStrings) {
            bw.write(s);
            bw.newLine();
        }
        bw.close();
    }

    @After
    public void afterTest() throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        fs.delete(HDFSResourceStore.hdfsPath(testPath, config), true);
        fs.delete(HDFSResourceStore.hdfsPath(testPath2, config), true);
    }

    @Test
    public void testSave() throws IOException {
        HDFSResourceStore resourceStore = new HDFSResourceStore(config);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (String s : testStrings) {
            baos.write(s.getBytes());
            baos.write("\n".getBytes());
        }
        baos.close();
        resourceStore.putResource(testPath2, new ByteArrayInputStream(baos.toByteArray()), 0);

        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        FSDataInputStream inputStream = fs.open(HDFSResourceStore.hdfsPath(testPath2, config));
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(inputStream));

        try {
            for (String s : testStrings) {
                Assert.assertEquals(bufferedReader.readLine(), s);
            }
        } finally {
            bufferedReader.close();
        }
    }

    @Test
    public void testLoad() throws IOException {
        HDFSResourceStore resourceStore = new HDFSResourceStore(config);
        RawResource rawResource = resourceStore.getResource(testPath);
        BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(rawResource.inputStream));

        try {
            for (String s : testStrings) {
                Assert.assertEquals(s, bufferedReader.readLine());
            }
        } finally {
            bufferedReader.close();
        }
    }
}
