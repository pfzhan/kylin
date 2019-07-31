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

package org.apache.kylin.common.util;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.Writable;
import org.apache.kylin.common.storage.IStorageProvider;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class HadoopUtil {

    public static final String JOB_TMP_ROOT = "/job_tmp";
    public static final String PARQUET_STORAGE_ROOT = "/parquet";
    public static final String DICT_STORAGE_ROOT = "/dict";
    public static final String GLOBAL_DICT_STORAGE_ROOT = DICT_STORAGE_ROOT + "/global_dict";
    public static final String SNAPSHOT_STORAGE_ROOT = "/table_snapshot";
    public static final String TABLE_EXD_STORAGE_ROOT = ResourceStore.TABLE_EXD_RESOURCE_ROOT;

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(HadoopUtil.class);
    private static final transient ThreadLocal<Configuration> hadoopConfig = new ThreadLocal<>();

    public static void setCurrentConfiguration(Configuration conf) {
        hadoopConfig.set(conf);
    }

    private static final String FILE_PREFIX = "file://";
    private static final String MAPR_FS_PREFIX = "maprfs://";

    public static Configuration getCurrentConfiguration() {
        if (hadoopConfig.get() == null) {
            Configuration conf = healSickConfig(new Configuration());
            // do not cache this conf, or will affect following mr jobs
            return conf;
        }
        Configuration conf = hadoopConfig.get();
        return conf;
    }

    public static Configuration newLocalConfiguration() {
        Configuration conf = new Configuration(false);
        conf.set("fs.default.name", "file:///");
        return conf;
    }

    public static Configuration healSickConfig(Configuration conf) {
        // https://issues.apache.org/jira/browse/KYLIN-953
        if (StringUtils.isBlank(conf.get("hadoop.tmp.dir"))) {
            conf.set("hadoop.tmp.dir", "/tmp");
        }
        //  https://issues.apache.org/jira/browse/KYLIN-3064
        conf.set("yarn.timeline-service.enabled", "false");

        return conf;
    }

    public static FileSystem getWorkingFileSystem() {
        return getFileSystem(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(null));
    }

    public static FileSystem getWorkingFileSystem(Configuration conf) {
        Path workingPath = new Path(KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory(null));
        return getFileSystem(workingPath, conf);
    }

    public static FileSystem getReadFileSystem() {
        Configuration conf = getCurrentConfiguration();
        return getReadFileSystem(conf);
    }

    public static FileSystem getReadFileSystem(Configuration conf) {
        Path parquetReadPath = new Path(KylinConfig.getInstanceFromEnv().getReadHdfsWorkingDirectory(null));
        return getFileSystem(parquetReadPath, conf);
    }

    public static FileSystem getFileSystem(String path) {
        return getFileSystem(new Path(makeURI(path)));
    }

    public static FileSystem getFileSystem(Path path) {
        Configuration conf = getCurrentConfiguration();
        return getFileSystem(path, conf);
    }

    public static FileSystem getFileSystem(Path path, Configuration conf) {
        try {
            return path.getFileSystem(conf);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static URI makeURI(String filePath) {
        try {
            return new URI(fixWindowsPath(filePath));
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Cannot create FileSystem from URI: " + filePath, e);
        }
    }

    public static String fixWindowsPath(String path) {
        // fix windows path
        if (path.startsWith("C:\\") || path.startsWith("D:\\")) {
            path = "file:///" + path;
        } else if (path.startsWith("C:/") || path.startsWith("D:/")) {
            path = "file:///" + path;
        } else if (path.startsWith(FILE_PREFIX) && !path.startsWith("file:///") && path.contains(":\\")) {
            path = path.replace(FILE_PREFIX, "file:///");
        }

        if (path.startsWith("file:///")) {
            path = path.replace('\\', '/');
        }
        return path;
    }

    /**
     * @param table the identifier of hive table, in format <db_name>.<table_name>
     * @return a string array with 2 elements: {"db_name", "table_name"}
     */
    public static String[] parseHiveTableName(String table) {
        int cut = table.indexOf('.');
        String database = cut >= 0 ? table.substring(0, cut).trim() : "DEFAULT";
        String tableName = cut >= 0 ? table.substring(cut + 1).trim() : table.trim();

        return new String[] { database, tableName };
    }

    public static void deletePath(Configuration conf, Path path) throws IOException {
        FileSystem fs = FileSystem.get(path.toUri(), conf);
        if (fs.exists(path)) {
            fs.delete(path, true);
        }
    }

    public static byte[] toBytes(Writable writable) {
        try {
            ByteArrayOutputStream bout = new ByteArrayOutputStream();
            DataOutputStream out = new DataOutputStream(bout);
            writable.write(out);
            out.close();
            bout.close();
            return bout.toByteArray();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path getFilterOnlyPath(FileSystem fs, Path baseDir, final String filter) throws IOException {
        if (fs.exists(baseDir) == false) {
            return null;
        }

        FileStatus[] fileStatus = fs.listStatus(baseDir, new PathFilter() {
            @Override
            public boolean accept(Path path) {
                return path.getName().startsWith(filter);
            }
        });

        if (fileStatus.length == 1) {
            return fileStatus[0].getPath();
        } else {
            return null;
        }
    }

    public static String getBackupFolder(KylinConfig kylinConfig) {
        return kylinConfig.getHdfsWorkingDirectory() + "_backup";
    }

    public static String getPathWithoutScheme(String path) {
        if (path.startsWith(FILE_PREFIX) || path.startsWith(MAPR_FS_PREFIX))
            return path;

        if (path.startsWith("file:")) {
            path = path.replace("file:", FILE_PREFIX);
        } else if (path.startsWith("maprfs:")) {
            path = path.replace("maprfs:", MAPR_FS_PREFIX);
        } else {
            path = Path.getPathWithoutSchemeAndAuthority(new Path(path)).toString() + "/";
        }
        return path;
    }

    public static ContentSummary getContentSummary(FileSystem fileSystem, Path path) throws IOException {
        IStorageProvider provider = (IStorageProvider) ClassUtil
                .newInstance(KylinConfig.getInstanceFromEnv().getStorageProvider());
        logger.debug("Use provider:{}", provider.getClass().getCanonicalName());
        return provider.getContentSummary(fileSystem, path);
    }
}
