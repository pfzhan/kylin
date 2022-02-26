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
package io.kyligence.kap.common.util;

import java.io.File;
import java.io.IOException;
import java.util.zip.ZipFile;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.util.ZipFileUtils;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import lombok.val;

public class ZipFileUtilTest {

    @Test
    public void testCompressAndDecompressZipFile(@TempDir File tempDir) throws IOException {
        String mainDir = tempDir.getAbsolutePath() + "/testCompressZipFile";

        File compressDir = new File(mainDir, "compress_dir");
        FileUtils.forceMkdir(compressDir);

        FileUtils.writeStringToFile(new File(compressDir, "a.txt"), "111111111111");
        FileUtils.writeStringToFile(new File(compressDir, "b.txt"), "222222222222");
        FileUtils.forceMkdir(new File(compressDir, "c"));
        FileUtils.writeStringToFile(new File(compressDir, "c/c1.txt"), "333333333333");

        String zipFilename = compressDir.getAbsolutePath() + ".zip";
        ZipFileUtils.compressZipFile(compressDir.getAbsolutePath(), zipFilename);

        Assert.assertTrue(new File(zipFilename).exists() && new File(zipFilename).length() > 200);

        File decompressDir = new File(mainDir, "decompress_dir");
        FileUtils.forceMkdir(decompressDir);
        ZipFileUtils.decompressZipFile(zipFilename, decompressDir.getAbsolutePath());

        val aFile = new File(decompressDir.getAbsolutePath(), "compress_dir/a.txt");
        val c1File = new File(decompressDir.getAbsolutePath(), "compress_dir/c/c1.txt");
        Assert.assertTrue(aFile.exists());
        Assert.assertEquals("111111111111", FileUtils.readFileToString(aFile));
        Assert.assertEquals("333333333333", FileUtils.readFileToString(c1File));
    }

    @Test
    public void testCompressEmptyDirZipFile(@TempDir File tempDir) throws IOException {
        String mainDir = tempDir.getAbsolutePath() + "/testCompressZipFile";

        File compressDir = new File(mainDir, "compress_dir");
        FileUtils.forceMkdir(compressDir);

        FileUtils.writeStringToFile(new File(compressDir, "a.txt"), "111111111111");
        FileUtils.writeStringToFile(new File(compressDir, "b.txt"), "222222222222");
        File emptyDirectory = new File(compressDir, "empty_directory");
        emptyDirectory.mkdir();
        String zipFilename = compressDir.getAbsolutePath() + ".zip";
        ZipFileUtils.compressZipFile(compressDir.getAbsolutePath(), zipFilename);

        long fileCount = new ZipFile(zipFilename).stream().count();
        Assert.assertEquals(3, fileCount);
    }
}
