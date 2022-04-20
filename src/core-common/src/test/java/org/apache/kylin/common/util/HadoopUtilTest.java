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
package org.apache.kylin.common.util;

import static org.apache.kylin.common.util.HadoopUtil.MAPR_FS_PREFIX;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import io.kyligence.kap.junit.annotation.MetadataInfo;
import lombok.val;

@MetadataInfo(project = "ssb")
class HadoopUtilTest {

    @TempDir
    Path tempDir;

    @Test
    void testGetFileStatusPathsFromHDFSDir_Dir() throws IOException {
        File mainDir = new File(tempDir.toFile(), "tt");
        FileUtils.forceMkdir(mainDir);
        Assertions.assertTrue(mainDir.exists());

        val fileStatus = HadoopUtil.getFileStatusPathsFromHDFSDir(tempDir.toString(), false);

        Assertions.assertEquals(1, fileStatus.size());
        Assertions.assertTrue(fileStatus.get(0).isDirectory());
    }

    @Test
    void testGetFileStatusPathsFromHDFSDIR_File() throws IOException {
        File tmpFile = new File(tempDir.toFile(), "abc.log");

        Assertions.assertTrue(tmpFile.createNewFile());
        Assertions.assertTrue(tmpFile.exists());

        val fileStatus = HadoopUtil.getFileStatusPathsFromHDFSDir(tempDir.toString(), true);

        Assertions.assertEquals(1, fileStatus.size());
        Assertions.assertTrue(fileStatus.get(0).isFile());
    }

    @Test
    void testMkdirIfNotExist_NotExist() {
        File mainDir = new File(tempDir.toFile(), "tt");

        Assertions.assertFalse(mainDir.exists());

        HadoopUtil.mkdirIfNotExist(mainDir.getAbsolutePath());

        Assertions.assertTrue(mainDir.exists());
        Assertions.assertTrue(mainDir.isDirectory());
    }

    @Test
    void testMkdirIfNotExist_Existed() throws IOException {
        File mainDir = new File(tempDir.toFile(), "tt");
        FileUtils.forceMkdir(mainDir);
        Assertions.assertTrue(mainDir.exists());

        HadoopUtil.mkdirIfNotExist(mainDir.getAbsolutePath());

        Assertions.assertTrue(mainDir.exists());
        Assertions.assertTrue(mainDir.isDirectory());
    }

    @Test
    void testDeletePath() throws IOException {
        File mainDir = new File(tempDir.toFile(), "testDeletePath");
        {
            Assertions.assertFalse(mainDir.exists());
            val deleteSuccess = HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(),
                    new org.apache.hadoop.fs.Path(mainDir.getAbsolutePath()));
            Assertions.assertFalse(deleteSuccess);
        }

        {
            FileUtils.forceMkdir(mainDir);
            Assertions.assertTrue(mainDir.exists());
            val deleteSuccess = HadoopUtil.deletePath(HadoopUtil.getCurrentConfiguration(),
                    new org.apache.hadoop.fs.Path(mainDir.getAbsolutePath()));
            Assertions.assertTrue(deleteSuccess);
        }
    }

    @Test
    void testGetPathWithoutScheme() {
        {
            val pathStr = "file://asdasd";
            val path = HadoopUtil.getPathWithoutScheme(pathStr);
            Assertions.assertEquals(pathStr, path);
        }

        {
            val pathStr = "file:/asdasd";
            val path = HadoopUtil.getPathWithoutScheme(pathStr);
            Assertions.assertEquals("file:///asdasd", path);
        }

        {
            val pathStr = MAPR_FS_PREFIX + "asdasd";
            val path = HadoopUtil.getPathWithoutScheme(pathStr);
            Assertions.assertEquals("asdasd", path);
        }

        {
            val pathStr = "xxx://asdasd";
            val path = HadoopUtil.getPathWithoutScheme(pathStr);
            Assertions.assertEquals("xxx://asdasd/", path);
        }
    }

    @Test
    void testToBytes() {
        val arrayWritable = new ArrayWritable(new String[] { "a" });
        val resultBytes = HadoopUtil.toBytes(arrayWritable);
        Assertions.assertNotNull(resultBytes);
    }

    @Test
    void testFixWindowsPath() {
        {
            val pathStr = "C:\\\\//asdasd";
            val path = HadoopUtil.fixWindowsPath(pathStr);
            Assertions.assertEquals("file:///C:////asdasd", path);
        }

        {
            val pathStr = "D:\\\\//asdasd";
            val path = HadoopUtil.fixWindowsPath(pathStr);
            Assertions.assertEquals("file:///D:////asdasd", path);
        }

        {
            val pathStr = "C:///asdasd";
            val path = HadoopUtil.fixWindowsPath(pathStr);
            Assertions.assertEquals("file:///C:///asdasd", path);
        }

        {
            val pathStr = "D:///asdasd";
            val path = HadoopUtil.fixWindowsPath(pathStr);
            Assertions.assertEquals("file:///D:///asdasd", path);
        }
    }

    @Test
    void testMakeURI() {
        {
            val pathStr = "C:\\\\//asdasd";
            val path = HadoopUtil.makeURI(pathStr);
            Assertions.assertEquals("file:///C:////asdasd", path.toString());
        }

        {
            val pathStr = "D:\\\\//asdasd";
            val path = HadoopUtil.makeURI(pathStr);
            Assertions.assertEquals("file:///D:////asdasd", path.toString());
        }

        {
            val pathStr = "C:///asdasd";
            val path = HadoopUtil.makeURI(pathStr);
            Assertions.assertEquals("file:///C:///asdasd", path.toString());
        }

        {
            val pathStr = "D:///asdasd";
            val path = HadoopUtil.makeURI(pathStr);
            Assertions.assertEquals("file:///D:///asdasd", path.toString());
        }
    }
}
