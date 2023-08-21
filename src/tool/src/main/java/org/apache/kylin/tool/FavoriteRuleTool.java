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
package org.apache.kylin.tool;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.Charset;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.metadata.project.NProjectManager;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.metadata.favorite.FavoriteRule;
import io.kyligence.kap.metadata.favorite.FavoriteRuleManager;

public class FavoriteRuleTool extends CancelableTask {
    private static final Logger logger = LoggerFactory.getLogger("diag");
    private static final String FAVORITE_RULE_DIR = "favorite_rule";
    private static final String ZIP_SUFFIX = ".zip";

    public void backup(String dir, String project) throws IOException {
        extractToHDFS(dir + "/" + FAVORITE_RULE_DIR, project);
    }

    public void restore(String dir, boolean afterTruncate) throws IOException {
        Path path = new Path(dir + "/" + FAVORITE_RULE_DIR);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        for (FileStatus fileStatus : fs.listStatus(path)) {
            String fileName = fileStatus.getPath().getName();
            String project = fileName.substring(0, fileName.indexOf("."));
            restoreProject(dir, project, afterTruncate);
        }
    }

    public void restoreProject(String dir, String project, boolean afterTruncate) throws IOException {
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        Path path = new Path(dir + "/" + FAVORITE_RULE_DIR + "/" + project + ZIP_SUFFIX);
        FavoriteRuleManager manager = FavoriteRuleManager.getInstance(project);
        List<FavoriteRule> rules = Lists.newArrayList();
        try(ZipInputStream zis = new ZipInputStream(fs.open(path));
            BufferedReader br = new BufferedReader(new InputStreamReader(zis))) {
            while (zis.getNextEntry() != null) {
                String value = br.readLine();
                FavoriteRule rule = JsonUtil.readValue(value, FavoriteRule.class);
                rules.add(rule);
            }
        }
        JdbcUtil.withTxAndRetry(manager.getTransactionManager(), () -> {
            if (afterTruncate) {
                for (FavoriteRule favoriteRule : manager.listAll()) {
                    manager.delete(favoriteRule);
                }
            }
            for (FavoriteRule rule : rules) {
                manager.updateRule(rule);
            }
            return null;
        });
    }

    public void extractFull(File dir) throws IOException {
        List<ProjectInstance> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv())
                .listAllProjects();
        for (ProjectInstance project : projects) {
            extractProject(dir, project.getName());
        }
    }

    public void extractProject(File dir, String project) throws IOException {
        File projectFile = new File(dir, project);
        FavoriteRuleManager manager = FavoriteRuleManager.getInstance(project);
        try (OutputStream os = new FileOutputStream(projectFile);
                BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(os, Charset.defaultCharset()))) {
            for (FavoriteRule line : manager.getAll()) {
                try {
                    bw.write(JsonUtil.writeValueAsString(line));
                    bw.newLine();
                    if (isCanceled()) {
                        logger.info("favorite rule backup was canceled.");
                        return;
                    }
                } catch (Exception e) {
                    logger.error("Write error, id is {}", line.getId(), e);
                }
            }
        }
    }

    public void extractToHDFS(String dir, String project) throws IOException {
        FavoriteRuleManager manager = FavoriteRuleManager.getInstance(project);
        FileSystem fs = HadoopUtil.getWorkingFileSystem();
        String filePathStr = dir + "/" + project + ZIP_SUFFIX;
        try (FSDataOutputStream fos = fs.create(new Path(filePathStr));
                        ZipOutputStream zos = new ZipOutputStream(fos);
                        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(zos, Charset.defaultCharset()))) {
            for (FavoriteRule line : manager.getAll()) {
                zos.putNextEntry(new ZipEntry(line.getId() + ".json"));
                bw.write(JsonUtil.writeValueAsString(line));
                bw.flush();
            }
        }
    }
}
