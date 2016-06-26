/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *  
 *     http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
