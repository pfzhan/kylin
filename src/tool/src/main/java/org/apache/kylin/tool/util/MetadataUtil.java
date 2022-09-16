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
package org.apache.kylin.tool.util;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.stream.Collectors;

import javax.sql.DataSource;

import org.apache.commons.dbcp2.BasicDataSource;
import org.apache.commons.io.FileUtils;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.logging.LogOutputStream;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.metadata.JdbcDataSource;
import org.apache.kylin.common.persistence.metadata.MetadataStore;
import org.apache.kylin.common.persistence.metadata.jdbc.JdbcUtil;
import org.apache.kylin.common.persistence.transaction.UnitOfWorkParams;
import org.apache.kylin.job.execution.DumpInfo;
import org.apache.kylin.metadata.cube.model.NDataflowManager;
import org.apache.kylin.metadata.project.EnhancedUnitOfWork;
import org.apache.kylin.metadata.project.NProjectManager;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class MetadataUtil {

    private static final Charset DEFAULT_CHARSET = Charset.defaultCharset();
    private static final String EMPTY = "";

    private MetadataUtil() {
    }

    public static String getMetadataUrl(String rootPath) {
        if (rootPath.startsWith("file://")) {
            rootPath = rootPath.replace("file://", "");
        }
        return org.apache.commons.lang3.StringUtils.appendIfMissing(rootPath, "/");
    }

    public static DataSource getDataSource(KylinConfig kylinConfig) throws Exception {
        val url = kylinConfig.getMetadataUrl();
        val props = JdbcUtil.datasourceParameters(url);

        return JdbcDataSource.getDataSource(props);
    }

    public static void createTableIfNotExist(BasicDataSource dataSource, String tableName, String tableSql,
            List<String> indexSqlList) throws IOException, SQLException {
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            return;
        }

        if (null == indexSqlList) {
            indexSqlList = Lists.newArrayList();
        }

        Properties properties = JdbcUtil.getProperties(dataSource);
        String createTableStmt = String.format(Locale.ROOT, properties.getProperty(tableSql), tableName);
        List<String> crateIndexStmtList = indexSqlList.stream()
                .map(indexSql -> String.format(Locale.ROOT, properties.getProperty(indexSql), tableName, tableName))
                .collect(Collectors.toList());
        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.setStopOnError(true);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(createTableStmt.getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
            crateIndexStmtList.forEach(crateIndexStmt -> sr.runScript(new InputStreamReader(
                    new ByteArrayInputStream(crateIndexStmt.getBytes(DEFAULT_CHARSET)), DEFAULT_CHARSET)));

        }
    }

    public static void createTableIfNotExist(BasicDataSource dataSource, String tableName, String createTableStmt)
            throws SQLException {
        if (JdbcUtil.isTableExists(dataSource.getConnection(), tableName)) {
            return;
        }

        try (Connection connection = dataSource.getConnection()) {
            ScriptRunner sr = new ScriptRunner(connection);
            sr.setLogWriter(new PrintWriter(new OutputStreamWriter(new LogOutputStream(log), DEFAULT_CHARSET)));
            sr.setStopOnError(true);
            sr.runScript(new InputStreamReader(new ByteArrayInputStream(createTableStmt.getBytes(DEFAULT_CHARSET)),
                    DEFAULT_CHARSET));
        }
    }

    public static void attachMetadataAndKylinProps(DumpInfo info) throws Exception {
        KylinConfig config = getConfigForSparkJob(info);
        String metaDumpUrl = info.getDistMetaUrl();

        if (org.apache.commons.lang.StringUtils.isEmpty(metaDumpUrl)) {
            throw new RuntimeException("Missing metaUrl");
        }

        File tmpDir = File.createTempFile("kylin_job_meta", EMPTY);
        FileUtils.forceDelete(tmpDir); // we need a directory, so delete the file first

        final Properties props = config.exportToProperties();
        // If we don't remove these configurations,
        // they will be overwritten in the SparkApplication
        props.setProperty("kylin.metadata.url", metaDumpUrl);

        removeUnNecessaryDump(props);
        KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
        MetadataStore dstMetadataStore = MetadataStore.createMetadataStore(dstConfig);

        ResourceStore.dumpKylinProps(tmpDir, props);
        if (!info.isKylinPropsOnly()) {
            if (info.getType() == DumpInfo.DumpType.DATA_LOADING) {
                dumpMetadataToTmpDir(config, info, tmpDir);
            } else if (info.getType() == DumpInfo.DumpType.ASYNC_QUERY) {
                dstMetadataStore.dump(ResourceStore.getKylinMetaStore(config), info.getMetadataDumpList());
            }
        }

        // copy metadata to target metaUrl
        MetadataStore.createMetadataStore(dstConfig).uploadFromFile(tmpDir);
        // clean up
        log.debug("Copied metadata to the target metaUrl, delete the temp dir: {}", tmpDir);
        FileUtils.forceDelete(tmpDir);
    }
    
    private static void dumpMetadataToTmpDir(KylinConfig config, DumpInfo info, File tmpDir) {
        // The way of Updating metadata is CopyOnWrite. So it is safe to use Reference in the value.
        Map<String, RawResource> dumpMap = EnhancedUnitOfWork
                .doInTransactionWithCheckAndRetry(UnitOfWorkParams.<Map<String, RawResource>> builder().readonly(true)
                        .unitName(info.getProject()).maxRetry(1).processor(() -> {
                            Map<String, RawResource> retMap = Maps.newHashMap();
                            for (String resPath : info.getMetadataDumpList()) {
                                ResourceStore resourceStore = ResourceStore.getKylinMetaStore(config);
                                RawResource rawResource = resourceStore.getResource(resPath);
                                retMap.put(resPath, rawResource);
                            }
                            return retMap;
                        }).build());

        if (Objects.isNull(dumpMap) || dumpMap.isEmpty()) {
            return;
        }
        // dump metadata
        ResourceStore.dumpResourceMaps(config, tmpDir, dumpMap);
    }

    private static KylinConfig getConfigForSparkJob(DumpInfo info) {
        val originalConfig = KylinConfig.getInstanceFromEnv();
        if (!originalConfig.isDevOrUT() && !checkHadoopWorkingDir()) {
            KylinConfig.getInstanceFromEnv().reloadKylinConfigPropertiesFromSiteProperties();
        }
        KylinConfigExt kylinConfigExt = null;
        Preconditions.checkState(org.apache.commons.lang.StringUtils.isNotBlank(info.getProject()),
                "job " + info.getJobId() + " project info is empty");
        if (org.apache.commons.lang.StringUtils.isNotBlank(info.getDataflow())) {
            val dataflowManager = NDataflowManager.getInstance(originalConfig, info.getProject());
            kylinConfigExt = dataflowManager.getDataflow(info.getDataflow()).getConfig();
        } else {
            val projectInstance = NProjectManager.getInstance(originalConfig).getProject(info.getProject());
            kylinConfigExt = projectInstance.getConfig();
        }
        return KylinConfigExt.createInstance(kylinConfigExt, info.getOverrideProps());
    }

    private static Boolean checkHadoopWorkingDir() {
        // read hdfs.working.dir in kylin config
        final KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        final String hdfsWorkingDirectory = kylinConfig.getHdfsWorkingDirectory();
        // read hdfs.working.dir
        final Properties properties = KylinConfig.buildSiteProperties();
        final String hdfsWorkingDirectoryFromProperties = kylinConfig.getHdfsWorkingDirectoryFromProperties(properties);
        return org.apache.commons.lang.StringUtils.equals(hdfsWorkingDirectory, hdfsWorkingDirectoryFromProperties);
    }

    private static void removeUnNecessaryDump(Properties props) {
        // Rewrited thru '--jars'.
        props.remove("kylin.engine.spark-conf.spark.jars");
        props.remove("kylin.engine.spark-conf.spark.yarn.dist.jars");
        // Rewrited thru '--files'.
        props.remove("kylin.engine.spark-conf.spark.files");
        props.remove("kylin.engine.spark-conf.spark.yarn.dist.files");

        // Rewrited.
        props.remove("kylin.engine.spark-conf.spark.driver.extraJavaOptions");
        props.remove("kylin.engine.spark-conf.spark.yarn.am.extraJavaOptions");
        props.remove("kylin.engine.spark-conf.spark.executor.extraJavaOptions");

        // Rewrited.
        props.remove("kylin.engine.spark-conf.spark.driver.extraClassPath");
        props.remove("kylin.engine.spark-conf.spark.executor.extraClassPath");

        props.remove("kylin.query.async-query.spark-conf.spark.yarn.am.extraJavaOptions");
        props.remove("kylin.query.async-query.spark-conf.spark.executor.extraJavaOptions");

        props.remove("kylin.storage.columnar.spark-conf.spark.yarn.am.extraJavaOptions");
        props.remove("kylin.storage.columnar.spark-conf.spark.executor.extraJavaOptions");
    }
}
