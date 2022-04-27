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


package io.kyligence.kap.rest.util;

import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Date;
import java.util.Objects;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TimestampedRollingFileOutputDirTest {

    private static final Logger logger = LoggerFactory.getLogger("kybot");

    private static final String FILE_NAME_PREFIX = "dummy.";

    private File outputDir;

    @Before
    public void setup() throws Exception {
        outputDir = Files.createTempDirectory("TimestampedRollingFileOutputDirTest").toFile();
        logger.debug("created tmp dir {}", outputDir);
    }

    @After
    public void tearDown() throws Exception {
        FileUtils.deleteDirectory(outputDir);
    }

    private TimestampedRollingFileOutputDir newRollingOutputDir(int maxFileCount) {
        return new TimestampedRollingFileOutputDir(outputDir, FILE_NAME_PREFIX, maxFileCount);
    }

    private void validateCreatedFile(File file) {
        Assert.assertTrue(file.getPath().startsWith(outputDir.getPath()));
        Assert.assertTrue(file.getName().startsWith(FILE_NAME_PREFIX));

        long fileTS = Long.parseLong(file.getName().substring(FILE_NAME_PREFIX.length()));
        Assert.assertTrue(fileTS > new Date(0).getTime());
        Assert.assertTrue(fileTS <= System.currentTimeMillis());
    }

    private File newFile(TimestampedRollingFileOutputDir rollingFileOutputDir) throws IOException {
        long start = System.currentTimeMillis();
        await().until(() -> start != System.currentTimeMillis());
        return rollingFileOutputDir.newOutputFile();
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCreationParamsEmptyName() {
        new TimestampedRollingFileOutputDir(outputDir, "", 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidCreationParamsInvalidMaxFileCnt() {
        new TimestampedRollingFileOutputDir(outputDir, FILE_NAME_PREFIX, 0);
    }

    @Test
    public void testNewFileCreation() throws IOException {
        TimestampedRollingFileOutputDir rollingFileOutputDir = newRollingOutputDir(1);
        File file = newFile(rollingFileOutputDir);

        Assert.assertEquals(1, Objects.requireNonNull(outputDir.listFiles()).length);
        validateCreatedFile(file);
    }

    @Test
    public void testFileRollingCount1() throws IOException {
        TimestampedRollingFileOutputDir rollingFileOutputDir = newRollingOutputDir(1);
        File file1 = newFile(rollingFileOutputDir);
        Assert.assertEquals(1, Objects.requireNonNull(outputDir.listFiles()).length);
        File file2 = newFile(rollingFileOutputDir);
        Assert.assertEquals(1, Objects.requireNonNull(outputDir.listFiles()).length);
        Assert.assertNotEquals(file1.getPath(), file2.getPath());
        Assert.assertFalse(file1.exists());
        Assert.assertTrue(file2.exists());
    }

    @Test
    public void testFileRollingCount2() throws IOException {
        TimestampedRollingFileOutputDir rollingFileOutputDir = newRollingOutputDir(2);
        File file1 = newFile(rollingFileOutputDir);
        File file2 = newFile(rollingFileOutputDir);
        Assert.assertEquals(2, Objects.requireNonNull(outputDir.listFiles()).length);

        File file3 = newFile(rollingFileOutputDir);
        Assert.assertEquals(2, Objects.requireNonNull(outputDir.listFiles()).length);

        Assert.assertFalse(file1.exists());
        Assert.assertTrue(file2.exists());
        Assert.assertTrue(file3.exists());
    }

}
