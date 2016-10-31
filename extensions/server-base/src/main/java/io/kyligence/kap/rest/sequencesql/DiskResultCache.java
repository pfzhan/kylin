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

package io.kyligence.kap.rest.sequencesql;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.util.Arrays;

import javax.annotation.Nullable;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.WildcardFileFilter;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;

public class DiskResultCache {
    private static final Logger logger = LoggerFactory.getLogger(DiskResultCache.class);

    private File cacheFolder;

    public DiskResultCache() {
        try {
            this.cacheFolder = File.createTempFile("resultcache", null);
            this.cacheFolder.delete();
            boolean mkdirs = this.cacheFolder.mkdirs();
            if (!mkdirs) {
                throw new RuntimeException("Cannot create result cache directory: " + cacheFolder);
            } else {
                this.cacheFolder.deleteOnExit();
                logger.info("All the result cache for sequence sqls will be cached in " + this.cacheFolder);
            }
        } catch (IOException e) {
            throw new RuntimeException("Cannot create result cache directory: " + cacheFolder, e);
        }
    }

    public void cleanEntries(String prefix) {
        logger.info("Cleaning all entries starting with " + prefix);
        FileFilter fileFilter = new WildcardFileFilter(prefix + "*");
        File[] files = this.cacheFolder.listFiles(fileFilter);
        String fileList = StringUtils.join(Collections2.transform(Arrays.asList(files), new Function<File, String>() {
            @Nullable
            @Override
            public String apply(@Nullable File input) {
                return input.getName();
            }
        }), ",");
        logger.info("Deleting the following files: " + fileList);
        for (File file : files) {
            file.delete();
        }
    }

    public void cacheEntry(String key, byte[] data) {

        File file = new File(this.cacheFolder, key);
        try {
            logger.info("adding a new entry to {}", file.getAbsolutePath());
            FileUtils.writeByteArrayToFile(file, data);
        } catch (IOException e) {
            throw new RuntimeException("Failed to write data to file " + file, e);
        }
    }

    public byte[] getEntry(String key) {
        return getEntry(key, true);
    }

    public byte[] getEntry(String key, boolean allowNonExist) {
        File file = new File(this.cacheFolder, key);
        if (!file.exists()) {
            if (!allowNonExist) {
                throw new RuntimeException("The entry for key " + key + " does not exist");
            }
            return null;
        } else {
            try {
                byte[] data = FileUtils.readFileToByteArray(file);
                //update last modify time for avoid cleaning
                boolean b = file.setLastModified(System.currentTimeMillis());
                if (!b) {
                    throw new RuntimeException("Failed to modify last modified time for " + file);
                }
                return data;
            } catch (IOException e) {
                throw new RuntimeException("Failed to read data from file " + file, e);
            }
        }
    }

    public int getSize() {
        String[] list = this.cacheFolder.list();
        return list == null ? 0 : list.length;
    }
}
