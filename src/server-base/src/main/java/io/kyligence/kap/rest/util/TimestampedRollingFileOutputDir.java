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

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.Comparator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

public class TimestampedRollingFileOutputDir {
    private static final Logger logger = LoggerFactory.getLogger(TimestampedRollingFileOutputDir.class);
    private final File outputDir;
    private final String fileNamePrefix;
    private final long maxFileCount;

    public TimestampedRollingFileOutputDir(File outputDir, String fileNamePrefix, long maxFileCount) {
        this.outputDir = outputDir;
        Preconditions.checkArgument(!fileNamePrefix.isEmpty(), "fileNamePrefix cannot be empty");
        Preconditions.checkArgument(maxFileCount > 0, "maxFileCount cannot be less than 1");
        this.fileNamePrefix = normalizeFileNamePrefix(fileNamePrefix);
        this.maxFileCount = maxFileCount;
    }

    public File newOutputFile() throws IOException {
        File[] files = outputDir.listFiles(fileFilter());
        if (files == null) {
            throw new RuntimeException("Invalid output directory " + outputDir);
        }

        logger.debug("found {} output files under output dir", files.length);
        if (files.length > 0) {
            Arrays.sort(files, fileComparatorByAge());
            removeOldFiles(files);
        }

        File newFile = new File(outputDir, newFileName());
        if (!newFile.createNewFile()) {
            logger.warn("output file {} already exists", newFile);
        }
        logger.debug("created output file {}", newFile);
        return newFile;
    }

    private String normalizeFileNamePrefix(String fileNamePrefix) {
        return fileNamePrefix.endsWith(".") ? fileNamePrefix : fileNamePrefix + ".";
    }

    protected FileFilter fileFilter() {
        return new FileFilter() {
            @Override
            public boolean accept(File pathname) {
                return pathname.getName().startsWith(fileNamePrefix);
            }
        };
    }

    protected Comparator<File> fileComparatorByAge() {
        return new Comparator<File>() {
            @Override
            public int compare(File file1, File file2) {
                return file2.getName().compareTo(file1.getName());
            }
        };
    }

    protected void removeOldFiles(File[] files) {
        for (int i = files.length - 1; i > maxFileCount - 2; i--) {
            String filePath = files[i].getPath();
            try {
                Files.deleteIfExists(files[i].toPath());
                logger.debug("Removed outdated file {}", filePath);
            } catch (IOException e) {
                logger.error("Error removing outdated file {}", filePath, e);
            }
        }
    }

    protected String newFileName() {
        return fileNamePrefix + System.currentTimeMillis();
    }
}
