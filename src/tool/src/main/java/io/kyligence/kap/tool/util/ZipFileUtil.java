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
package io.kyligence.kap.tool.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Locale;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.exception.CommonErrorCode;
import org.apache.kylin.common.exception.KylinException;

import lombok.val;

public class ZipFileUtil {
    private ZipFileUtil() {
    }

    public static void decompressZipFile(String zipFilename, String targetDir) throws IOException {
        if (!validateZipFilename(zipFilename)) {
            throw new KylinException(CommonErrorCode.INVALID_ZIP_NAME, "Zipfile must end with .zip");
        }
        String normalizedTargetDir = Paths.get(targetDir).normalize().toString();
        try (ZipInputStream zipInputStream = new ZipInputStream(new FileInputStream(zipFilename))) {
            ZipEntry entry;
            while ((entry = zipInputStream.getNextEntry()) != null) {
                val entryDir = Paths.get(targetDir, entry.getName()).normalize().toString();
                if (!entryDir.startsWith(normalizedTargetDir)) {
                    throw new KylinException(CommonErrorCode.INVALID_ZIP_ENTRY,
                            "Zip Entry <" + entry.getName() + "> is Invalid");
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(Paths.get(entryDir));
                } else {
                    Files.createDirectories(Paths.get(entryDir).getParent());
                    Files.copy(zipInputStream, Paths.get(targetDir, entry.getName()));
                }
            }
        }
    }

    public static void compressZipFile(String sourceDir, String zipFilename) throws IOException {
        if (!validateZipFilename(zipFilename)) {
            throw new KylinException(CommonErrorCode.INVALID_ZIP_NAME, "Zipfile must end with .zip");
        }
        try (ZipOutputStream zipFile = new ZipOutputStream(new FileOutputStream(zipFilename))) {
            compressDirectoryToZipfile(normDir(new File(sourceDir).getParent()), normDir(sourceDir), zipFile);
        }
    }

    private static void compressDirectoryToZipfile(String rootDir, String sourceDir, ZipOutputStream out)
            throws IOException {
        File[] sourceFiles = new File(sourceDir).listFiles();
        if (null == sourceFiles) {
            return;
        }

        // should put empty directory to zip file
        if (sourceFiles.length == 0) {
            out.putNextEntry(
                    new ZipEntry(normDir(StringUtils.isEmpty(rootDir) ? sourceDir : sourceDir.replace(rootDir, ""))));
        }

        for (File sourceFile : sourceFiles) {
            if (sourceFile.isDirectory()) {
                compressDirectoryToZipfile(rootDir, sourceDir + normDir(sourceFile.getName()), out);
            } else {
                ZipEntry entry = new ZipEntry(
                        normDir(StringUtils.isEmpty(rootDir) ? sourceDir : sourceDir.replace(rootDir, ""))
                                + sourceFile.getName());
                entry.setTime(sourceFile.lastModified());
                out.putNextEntry(entry);

                FileInputStream in = new FileInputStream(sourceDir + sourceFile.getName());
                IOUtils.copy(in, out);
                IOUtils.closeQuietly(in);
            }
        }
    }

    public static boolean validateZipFilename(String filename) {
        return StringUtils.isNotBlank(filename) && filename.trim().toLowerCase(Locale.ROOT).endsWith(".zip");
    }

    private static String normDir(String dirName) {
        if (!StringUtils.isEmpty(dirName) && !dirName.endsWith(File.separator)) {
            dirName = dirName + File.separator;
        }
        return dirName;
    }
}
