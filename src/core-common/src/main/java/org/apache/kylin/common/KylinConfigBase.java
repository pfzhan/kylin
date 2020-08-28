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

package org.apache.kylin.common;

import static java.lang.Math.toIntExact;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.lock.curator.CuratorDistributedLockFactory;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.metadata.HDFSMetadataStore;
import io.kyligence.kap.common.util.ClusterConstant;

/**
 * An abstract class to encapsulate access to a set of 'properties'.
 * Subclass can override methods in this class to extend the content of the 'properties',
 * with some override values for example.
 */
public abstract class KylinConfigBase implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KylinConfigBase.class);

    private static final String WORKING_DIR_PROP = "kylin.env.hdfs-working-dir";
    private static final String DATA_WORKING_DIR_PROP = "kylin.env.hdfs-data-working-dir";
    private static final String KYLIN_ROOT = "/kylin";

    public static final long REJECT_SIMILARITY_THRESHOLD = 100_000_000L;
    public static final double SIMILARITY_THRESHOLD = 0.9;

    public static final long MINUTE = 60;

    public static final String TRUE = "true";
    public static final String FALSE = "false";
    public static final String QUERY_NODE = "query";
    public static final String PATH_DELIMITER = "/";

    public static final String DIAG_ID_PREFIX = "front_";

    /*
     * DON'T DEFINE CONSTANTS FOR PROPERTY KEYS!
     *
     * For 1), no external need to access property keys, all accesses are by public methods.
     * For 2), it's cumbersome to maintain constants at top and code at bottom.
     * For 3), key literals usually appear only once.
     */

    public static String getKylinHome() {
        String kylinHome = getKylinHomeWithoutWarn();
        if (StringUtils.isEmpty(kylinHome)) {
            logger.warn("KYLIN_HOME was not set");
        }
        return kylinHome;
    }

    public static String getKylinHomeWithoutWarn() {
        String kylinHome = System.getenv("KYLIN_HOME");
        if (StringUtils.isEmpty(kylinHome)) {
            kylinHome = System.getProperty("KYLIN_HOME");
        }
        return kylinHome;
    }

    public static String getKylinConfHome() {
        String confHome = System.getenv("KYLIN_CONF");
        if (StringUtils.isEmpty(confHome)) {
            confHome = System.getProperty("KYLIN_CONF");
        }
        return confHome;
    }

    public static String getSparkHome() {
        String sparkHome = System.getenv("SPARK_HOME");
        if (StringUtils.isNotEmpty(sparkHome)) {
            return sparkHome;
        }

        return getKylinHome() + File.separator + "spark";
    }

    // backward compatibility check happens when properties is loaded or updated
    static BackwardCompatibilityConfig BCC = new BackwardCompatibilityConfig();

    // ============================================================================

    volatile Properties properties = new Properties();

    public KylinConfigBase() {
        this(new Properties());
    }

    public KylinConfigBase(Properties props) {
        this.properties = BCC.check(props);
    }

    protected KylinConfigBase(Properties props, boolean force) {
        this.properties = force ? props : BCC.check(props);
    }

    protected final String getOptional(String prop) {
        return getOptional(prop, null);
    }

    protected String getOptional(String prop, String dft) {

        final String property = System.getProperty(prop);
        return property != null ? getSubstitutor().replace(property)
                : getSubstitutor().replace(properties.getProperty(prop, dft));
    }

    protected Properties getAllProperties() {
        return getProperties(null);
    }

    /**
     *
     * @param propertyKeys the collection of the properties; if null will return all properties
     * @return
     */
    protected Properties getProperties(Collection<String> propertyKeys) {
        final StrSubstitutor substitutor = getSubstitutor();

        Properties properties = new Properties();
        for (Entry<Object, Object> entry : this.properties.entrySet()) {
            if (propertyKeys == null || propertyKeys.contains(entry.getKey())) {
                properties.put(entry.getKey(), substitutor.replace((String) entry.getValue()));
            }
        }

        return properties;
    }

    protected StrSubstitutor getSubstitutor() {
        // env > properties
        final Map<String, Object> all = Maps.newHashMap();
        all.putAll((Map) properties);
        all.putAll(System.getenv());

        return new StrSubstitutor(all);
    }

    protected Properties getRawAllProperties() {
        return properties;
    }

    protected final Map<String, String> getPropertiesByPrefix(String prefix) {
        Map<String, String> result = Maps.newLinkedHashMap();
        for (Entry<Object, Object> entry : getAllProperties().entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(prefix)) {
                result.put(key.substring(prefix.length()), (String) entry.getValue());
            }
        }
        for (Entry<Object, Object> entry : System.getProperties().entrySet()) {
            String key = (String) entry.getKey();
            if (key.startsWith(prefix)) {
                result.put(key.substring(prefix.length()), (String) entry.getValue());
            }
        }
        return result;
    }

    protected final String[] getOptionalStringArray(String prop, String[] dft) {
        final String property = getOptional(prop);
        if (!StringUtils.isBlank(property)) {
            return property.split("\\s*,\\s*");
        } else {
            return dft;
        }
    }

    protected final String[] getSystemStringArray(String prop, String[] dft) {
        final String property = System.getProperty(prop);
        if (!StringUtils.isBlank(property)) {
            return property.split("\\s*,\\s*");
        } else {
            return dft;
        }
    }

    protected final int[] getOptionalIntArray(String prop, String[] dft) {
        String[] strArray = getOptionalStringArray(prop, dft);
        int[] intArray = new int[strArray.length];
        for (int i = 0; i < strArray.length; i++) {
            intArray[i] = Integer.parseInt(strArray[i]);
        }
        return intArray;
    }

    protected final String getRequired(String prop) {
        String r = getOptional(prop);
        if (StringUtils.isEmpty(r)) {
            throw new IllegalArgumentException("missing '" + prop + "' in conf/kylin.properties");
        }
        return r;
    }

    /**
     * Use with care, properties should be read-only. This is for testing only.
     */
    public final void setProperty(String key, String value) {
        logger.trace("KylinConfig was updated with " + key + "=" + value);
        properties.setProperty(BCC.check(key), value);
    }

    private void wrapKerberosInfo(String configName) {
        StringBuilder sb = new StringBuilder();
        if (properties.containsKey(configName)) {
            String conf = (String) properties.get(configName);
            if (StringUtils.contains(conf, "java.security.krb5.conf")) {
                return;
            }
            sb.append(conf);
        }

        if (Boolean.valueOf(this.getOptional("kylin.kerberos.enabled", FALSE))) {
            if (this.getOptional("kylin.kerberos.platform", "").equalsIgnoreCase(KapConfig.FI_PLATFORM)
                    || Boolean.valueOf(this.getOptional("kylin.env.zk-kerberos-enabled", FALSE))) {
                sb.append(String.format(" -Djava.security.krb5.conf=%s", "./__spark_conf__/__hadoop_conf__/krb5.conf"));
            }
        }

        setProperty(configName, sb.toString());
    }

    final protected void reloadKylinConfig(Properties properties) {
        this.properties = BCC.check(properties);
        setProperty("kylin.metadata.url.identifier", getMetadataUrlPrefix());
        setProperty("kylin.log.spark-executor-properties-file", getLogSparkExecutorPropertiesFile());
        setProperty("kylin.log.spark-driver-properties-file", getLogSparkDriverPropertiesFile());
        setProperty("kylin.log.spark-appmaster-properties-file", getLogSparkAppMasterPropertiesFile());

        wrapKerberosInfo("kylin.engine.spark-conf.spark.executor.extraJavaOptions");
        wrapKerberosInfo("kylin.engine.spark-conf.spark.yarn.am.extraJavaOptions");

        // https://github.com/kyligence/kap/issues/12654
        this.properties.put(WORKING_DIR_PROP,
                makeQualified(new Path(this.properties.getProperty(WORKING_DIR_PROP, KYLIN_ROOT))).toString());
        if (this.properties.getProperty(DATA_WORKING_DIR_PROP) != null) {
            this.properties.put(DATA_WORKING_DIR_PROP,
                    makeQualified(new Path(this.properties.getProperty(DATA_WORKING_DIR_PROP))).toString());
        }
    }

    private Map<Integer, String> convertKeyToInteger(Map<String, String> map) {
        Map<Integer, String> result = Maps.newLinkedHashMap();
        for (Entry<String, String> entry : map.entrySet()) {
            result.put(Integer.valueOf(entry.getKey()), entry.getValue());
        }
        return result;
    }

    // ============================================================================
    // ENV
    // ============================================================================

    public boolean isDevOrUT() {
        return isUTEnv() || isDevEnv();
    }

    public boolean isUTEnv() {
        return "UT".equals(getDeployEnv());
    }

    public boolean isDevEnv() {
        return "DEV".equals(getDeployEnv());
    }

    public String getDeployEnv() {
        return getOptional("kylin.env", "PROD");
    }

    private String cachedHdfsWorkingDirectory;

    public String getHdfsWorkingDirectoryWithoutScheme() {
        return HadoopUtil.getPathWithoutScheme(getHdfsWorkingDirectory());
    }

    private Path makeQualified(Path path) {
        try {
            FileSystem fs = path.getFileSystem(HadoopUtil.getCurrentConfiguration());
            return fs.makeQualified(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String getHdfsWorkingDirectory() {
        if (cachedHdfsWorkingDirectory != null)
            return cachedHdfsWorkingDirectory;

        String root = getOptional(DATA_WORKING_DIR_PROP, null);
        boolean compriseMetaId = false;

        if (root == null) {
            root = getOptional(WORKING_DIR_PROP, KYLIN_ROOT);
            compriseMetaId = true;
        }

        Path path = new Path(root);
        if (!path.isAbsolute())
            throw new IllegalArgumentException("kylin.env.hdfs-working-dir must be absolute, but got " + root);

        // make sure path is qualified
        path = makeQualified(path);

        if (compriseMetaId) {
            // if configuration WORKING_DIR_PROP_V2 dose not exist, append metadata-url prefix
            String metaId = getMetadataUrlPrefix().replace(':', '-').replace('/', '-');
            path = new Path(path, metaId);
        }

        root = path.toString();
        if (!root.endsWith("/"))
            root += "/";

        cachedHdfsWorkingDirectory = root;
        if (cachedHdfsWorkingDirectory.startsWith("file:")) {
            cachedHdfsWorkingDirectory = cachedHdfsWorkingDirectory.replace("file:", "file://");
        } else if (cachedHdfsWorkingDirectory.startsWith("maprfs:")) {
            cachedHdfsWorkingDirectory = cachedHdfsWorkingDirectory.replace("maprfs:", "maprfs://");
        }
        logger.info("Hdfs data working dir is setting to " + cachedHdfsWorkingDirectory);
        return cachedHdfsWorkingDirectory;
    }

    public String getKylinMetricsPrefix() {
        return getOptional("kylin.metrics.prefix", "KYLIN").toUpperCase();
    }

    public String getFirstDayOfWeek() {
        return getOptional("kylin.metadata.first-day-of-week", "monday");
    }

    public String getKylinMetricsActiveReservoirDefaultClass() {
        return getOptional("kylin.metrics.active-reservoir-default-class",
                "org.apache.kylin.metrics.lib.impl.StubReservoir");
    }

    public String getKylinSystemCubeSinkDefaultClass() {
        return getOptional("kylin.metrics.system-cube-sink-default-class",
                "org.apache.kylin.metrics.lib.impl.hive.HiveSink");
    }

    public boolean isKylinMetricsMonitorEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metrics.monitor-enabled", FALSE));
    }

    public String getZookeeperBasePath() {
        return getOptional("kylin.env.zookeeper-base-path", KYLIN_ROOT);
    }

    public String getClusterName() {
        return this.getOptional("kylin.server.cluster-name", getMetadataUrlPrefix());
    }

    public int getZKBaseSleepTimeMs() {
        long sleepTimeMs = TimeUtil.timeStringAs(getOptional("kylin.env.zookeeper-base-sleep-time", "3s"),
                TimeUnit.MILLISECONDS);
        return toIntExact(sleepTimeMs);
    }

    public int getZKMaxRetries() {
        return Integer.parseInt(getOptional("kylin.env.zookeeper-max-retries", "3"));
    }

    /**
     * A comma separated list of host:port pairs, each corresponding to a ZooKeeper server
     */
    public String getZookeeperConnectString() {
        String str = getOptional("kylin.env.zookeeper-connect-string");
        if (!StringUtils.isEmpty(str))
            return str;

        throw new RuntimeException("Please set 'kylin.env.zookeeper-connect-string' in kylin.properties");
    }

    public boolean isZookeeperAclEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.env.zookeeper-acl-enabled", FALSE));
    }

    public String getZKAuths() {
        return getOptional("kylin.env.zookeeper.zk-auth", "digest:ADMIN:KYLIN");
    }

    public String getZKAcls() {
        return getOptional("kylin.env.zookeeper.zk-acl", "world:anyone:rwcda");
    }

    public String getYarnStatusCheckUrl() {
        return getOptional("kylin.job.yarn-app-rest-check-status-url", null);
    }

    // ============================================================================
    // METADATA
    // ============================================================================

    public int getQueryConcurrentRunningThresholdForProject() {
        // by default there's no limitation
        return Integer.parseInt(getOptional("kylin.query.project-concurrent-running-threshold", "0"));
    }

    public boolean isAdminUserExportAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.web.export-allow-admin", TRUE));
    }

    public boolean isNoneAdminUserExportAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.web.export-allow-other", TRUE));
    }

    public StorageURL getMetadataUrl() {
        return StorageURL.valueOf(getOptional("kylin.metadata.url", "kylin_metadata@jdbc"));
    }

    public boolean isMetadataAuditLogEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.audit-log.enabled", TRUE));
    }

    public long getMetadataAuditLogMaxSize() {
        return Long.valueOf(getOptional("kylin.metadata.audit-log.max-size", "3000000"));
    }

    public boolean isMetadataWaitSyncEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.wait-sync-enabled", TRUE));
    }

    // for test only
    @VisibleForTesting
    public void setMetadataUrl(String metadataUrl) {
        setProperty("kylin.metadata.url", metadataUrl);
    }

    public String getMetadataUrlPrefix() {
        return getMetadataUrl().getIdentifier();
    }

    public Map<String, String> getMetadataStoreImpls() {
        Map<String, String> r = Maps.newLinkedHashMap();
        // ref constants in ISourceAware
        r.put("", "io.kyligence.kap.common.persistence.metadata.FileMetadataStore");
        r.put("hdfs", "io.kyligence.kap.common.persistence.metadata.HDFSMetadataStore");
        r.put("jdbc", "io.kyligence.kap.common.persistence.metadata.JdbcMetadataStore");
        r.putAll(getPropertiesByPrefix("kylin.metadata.resource-store-provider.")); // note the naming convention -- http://kylin.apache.org/development/coding_naming_convention.html
        return r;
    }

    public String[] getHdfsMetaStoreFileSystemSchemas() {
        return getOptionalStringArray("kylin.metadata.hdfs-compatible-schemas", //
                new String[] { "hdfs", "maprfs", "s3", "s3a", "wasb", "wasbs", "adl", "adls", "abfs", "abfss", "gs",
                        "oss" });
    }

    public String getSecurityProfile() {
        return getOptional("kylin.security.profile", "testing");
    }

    public String[] getRealizationProviders() {
        return getOptionalStringArray("kylin.metadata.realization-providers", //
                new String[] { "io.kyligence.kap.metadata.cube.model.NDataflowManager" });
    }

    public String[] getCubeDimensionCustomEncodingFactories() {
        return getOptionalStringArray("kylin.metadata.custom-dimension-encodings", new String[0]);
    }

    public Map<String, String> getCubeCustomMeasureTypes() {
        return getPropertiesByPrefix("kylin.metadata.custom-measure-types.");
    }

    public CuratorDistributedLockFactory getDistributedLockFactory() {
        String clsName = getOptional("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.common.lock.curator.CuratorDistributedLockFactory");
        return (CuratorDistributedLockFactory) ClassUtil.newInstance(clsName);
    }

    public boolean isCheckCopyOnWrite() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.check-copy-on-write", FALSE));
    }

    public String getServerPort() {
        return getOptional("server.port", "7070");
    }

    public String getChannel() {
        return getOptional("kylin.env.channel", "on-premises");
    }

    public boolean isServerHttpsEnabled() {
        return Boolean.valueOf(getOptional("kylin.server.https.enable", FALSE));
    }

    public int getServerHttpsPort() {
        return Integer.parseInt(getOptional("kylin.server.https.port", "7443"));
    }

    public String getServerHttpsKeyType() {
        return getOptional("kylin.server.https.keystore-type", "JKS");
    }

    public String getServerHttpsKeystore() {
        return getOptional("kylin.server.https.keystore-file", getKylinHome() + "/server/.keystore");
    }

    public String getServerHttpsKeyPassword() {
        return getOptional("kylin.server.https.keystore-password", "changeit");
    }

    public String getServerHttpsKeyAlias() {
        return getOptional("kylin.server.https.key-alias", null);
    }

    public boolean isSemiAutoMode() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.semi-automatic-mode", FALSE));
    }

    /**
     * expose computed column in the table metadata and select * queries
     * @return
     */
    public boolean exposeComputedColumn() {
        return Boolean.parseBoolean(getOptional("kylin.query.metadata.expose-computed-column", FALSE));
    }

    public String getCalciteQuoting() {
        return getOptional("kylin.query.calcite.extras-props.quoting", "DOUBLE_QUOTE");
    }

    // ============================================================================
    // DICTIONARY & SNAPSHOT
    // ============================================================================

    public boolean isSnapshotParallelBuildEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.snapshot.parallel-build-enabled", TRUE));
    }

    public int snapshotParallelBuildTimeoutSeconds() {
        return Integer.parseInt(getOptional("kylin.snapshot.parallel-build-timeout-seconds", "3600"));
    }

    public int getSnapshotMaxVersions() {
        return Integer.parseInt(getOptional("kylin.snapshot.max-versions", "3"));
    }

    public long getSnapshotVersionTTL() {
        return Long.parseLong(getOptional("kylin.snapshot.version-ttl", "259200000"));
    }

    public int getSnapshotShardSizeMB() {
        return Integer.parseInt(getOptional("kylin.snapshot.shard-size-mb", "128"));
    }

    public int getGlobalDictV2MinHashPartitions() {
        return Integer.parseInt(getOptional("kylin.dictionary.globalV2-min-hash-partitions", "1"));
    }

    public int getGlobalDictV2ThresholdBucketSize() {
        return Integer.parseInt(getOptional("kylin.dictionary.globalV2-threshold-bucket-size", "500000"));
    }

    public double getGlobalDictV2InitLoadFactor() {
        return Double.parseDouble(getOptional("kylin.dictionary.globalV2-init-load-factor", "0.5"));
    }

    public double getGlobalDictV2BucketOverheadFactor() {
        return Double.parseDouble(getOptional("kylin.dictionary.globalV2-bucket-overhead-factor", "1.5"));
    }

    public int getGlobalDictV2MaxVersions() {
        return Integer.parseInt(getOptional("kylin.dictionary.globalV2-max-versions", "3"));
    }

    public long getGlobalDictV2VersionTTL() {
        return Long.parseLong(getOptional("kylin.dictionary.globalV2-version-ttl", "259200000"));
    }

    public long getNullEncodingOptimizeThreshold() {
        return Long.parseLong(getOptional("kylin.dictionary.null-encoding-opt-threshold", "40000000"));
    }

    // ============================================================================
    // CUBE
    // ============================================================================

    public String getSegmentAdvisor() {
        return getOptional("kylin.cube.segment-advisor", "org.apache.kylin.cube.CubeSegmentAdvisor");
    }

    public long getCubeAggrGroupMaxCombination() {
        return Long.parseLong(getOptional("kylin.cube.aggrgroup.max-combination", "4096"));
    }

    public boolean getCubeAggrGroupIsMandatoryOnlyValid() {
        return Boolean.parseBoolean(getOptional("kylin.cube.aggrgroup.is-mandatory-only-valid", TRUE));
    }

    public int getLowFrequencyThreshold() {
        return Integer.parseInt(this.getOptional("kylin.cube.low-frequency-threshold", "0"));
    }

    public int getFrequencyTimeWindowInDays() {
        return Integer.parseInt(this.getOptional("kylin.cube.frequency-time-window", "30"));
    }

    public boolean isBaseCuboidAlwaysValid() {
        return Boolean.parseBoolean(this.getOptional("kylin.cube.aggrgroup.is-base-cuboid-always-valid", TRUE));
    }

    // ============================================================================
    // JOB
    // ============================================================================

    public String getJobTmpDir(String project) {
        return getHdfsWorkingDirectoryWithoutScheme() + project + "/job_tmp/";
    }

    public StorageURL getJobTmpMetaStoreUrl(String project, String jobId) {
        Map<String, String> params = new HashMap<>();
        params.put("path", getJobTmpDir(project) + getNestedPath(jobId) + "meta");
        return new StorageURL(getMetadataUrlPrefix(), HDFSMetadataStore.HDFS_SCHEME, params);
    }

    public String getJobTmpOutputStorePath(String project, String jobId) {
        return getJobTmpDir(project) + getNestedPath(jobId) + "/execute_output.json";
    }

    public Path getJobTmpShareDir(String project, String jobId) {
        String path = getJobTmpDir(project) + jobId + "/share/";
        return new Path(path);
    }

    public Path getJobTmpFlatTableDir(String project, String jobId) {
        String path = getJobTmpDir(project) + jobId + "/flat_table/";
        return new Path(path);
    }

    public Path getFlatTableDir(String project, String dataFlowId, String segmentId) {
        String path = getHdfsWorkingDirectoryWithoutScheme() + project + "/flat_table/" + dataFlowId + PATH_DELIMITER
                + segmentId;
        return new Path(path);
    }

    public Path getJobTmpViewFactTableDir(String project, String jobId) {
        String path = getJobTmpDir(project) + jobId + "/view_fact_table/";
        return new Path(path);
    }

    // a_b => a/b/
    private String getNestedPath(String id) {
        String[] ids = id.split("_");
        StringBuilder builder = new StringBuilder();
        for (String subId : ids) {
            builder.append(subId).append("/");
        }
        return builder.toString();
    }

    public CliCommandExecutor getCliCommandExecutor() {
        CliCommandExecutor exec = new CliCommandExecutor();
        if (getRunAsRemoteCommand()) {
            exec.setRunAtRemote(getRemoteHadoopCliHostname(), getRemoteHadoopCliPort(), getRemoteHadoopCliUsername(),
                    getRemoteHadoopCliPassword());
        }
        return exec;
    }

    public boolean getRunAsRemoteCommand() {
        return Boolean.parseBoolean(getOptional("kylin.job.use-remote-cli"));
    }

    public int getRemoteHadoopCliPort() {
        return Integer.parseInt(getOptional("kylin.job.remote-cli-port", "22"));
    }

    public String getRemoteHadoopCliHostname() {
        return getOptional("kylin.job.remote-cli-hostname");
    }

    public String getRemoteHadoopCliUsername() {
        return getOptional("kylin.job.remote-cli-username");
    }

    public String getRemoteHadoopCliPassword() {
        return getOptional("kylin.job.remote-cli-password");
    }

    public int getRemoteSSHPort() {
        return Integer.parseInt(getOptional("kylin.job.remote-ssh-port", "22"));
    }

    public String getRemoteSSHUsername() {
        return getOptional("kylin.job.ssh-username");
    }

    public String getRemoteSSHPassword() {
        return getOptional("kylin.job.ssh-password");
    }

    public String getCliWorkingDir() {
        return getOptional("kylin.job.remote-cli-working-dir");
    }

    public int getMaxConcurrentJobLimit() {
        return Integer.parseInt(getOptional("kylin.job.max-concurrent-jobs", "20"));
    }

    public Boolean getAutoSetConcurrentJob() {
        if (isDevOrUT()) {
            return Boolean.parseBoolean(getOptional("kylin.job.auto-set-concurrent-jobs", FALSE));
        }
        return Boolean.parseBoolean(getOptional("kylin.job.auto-set-concurrent-jobs", FALSE));
    }

    public double getMaxLocalConsumptionRatio() {
        return Double.parseDouble(getOptional("kylin.job.max-local-consumption-ratio", "0.5"));
    }

    public String[] getOverCapacityMailingList() {
        return getOptionalStringArray("kylin.capacity.notification-emails", new String[0]);
    }

    public boolean isOverCapacityNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.capacity.notification-enabled", FALSE));
    }

    public double getOverCapacityThreshold() {
        return Double.parseDouble(getOptional("kylin.capacity.over-capacity-threshold", "80")) / 100;
    }

    public boolean isMailEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-enabled", FALSE));
    }

    public boolean isStarttlsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-mail-enable-starttls", FALSE));
    }

    public String getSmtpPort() {
        return getOptional("kylin.job.notification-mail-port", "25");
    }

    public String getMailHost() {
        return getOptional("kylin.job.notification-mail-host", "");
    }

    public String getMailUsername() {
        return getOptional("kylin.job.notification-mail-username", "");
    }

    public String getMailPassword() {
        return getOptional("kylin.job.notification-mail-password", "");
    }

    public String getMailSender() {
        return getOptional("kylin.job.notification-mail-sender", "");
    }

    public String[] getAdminDls() {
        return getOptionalStringArray("kylin.job.notification-admin-emails", new String[0]);
    }

    public int getJobRetry() {
        return Integer.parseInt(getOptional("kylin.job.retry", "0"));
    }

    // retry interval in milliseconds
    public int getJobRetryInterval() {
        return Integer.parseInt(getOptional("kylin.job.retry-interval", "30000"));
    }

    public String[] getJobRetryExceptions() {
        return getOptionalStringArray("kylin.job.retry-exception-classes", new String[0]);
    }

    public String getJobControllerLock() {
        return getOptional("kylin.job.lock", "org.apache.kylin.storage.hbase.util.ZookeeperJobLock");
    }

    public Integer getSchedulerPollIntervalSecond() {
        return Integer.parseInt(getOptional("kylin.job.scheduler.poll-interval-second", "30"));
    }

    public boolean isFlatTableJoinWithoutLookup() {
        return Boolean.parseBoolean(getOptional("kylin.job.flat-table-join-without-lookup", FALSE));
    }

    public String getJobTrackingURLPattern() {
        return getOptional("kylin.job.tracking-url-pattern", "");
    }

    public boolean isJobLogPrintEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.log-print-enabled", TRUE));

    }

    public boolean isCheckQuotaStorageEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.check-quota-storage-enabled", TRUE));
    }
    // ============================================================================
    // SOURCE.HIVE
    // ============================================================================

    public int getDefaultSource() {
        return Integer.parseInt(getOptional("kylin.source.default", "0"));
    }

    public Map<Integer, String> getSourceEngines() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        // ref constants in ISourceAware

        // these sources no longer exists in Newten
        //        r.put(0, "org.apache.kylin.source.hive.HiveSource");
        //        r.put(1, "org.apache.kylin.source.kafka.KafkaSource");
        //        r.put(8, "org.apache.kylin.source.jdbc.JdbcSource");
        r.put(1, "io.kyligence.kap.engine.spark.source.kafka.NSparkKafkaSource");
        r.put(9, "io.kyligence.kap.engine.spark.source.NSparkDataSource");
        r.put(13, "io.kyligence.kap.source.file.FileSource");

        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.source.provider.")));
        return r;
    }

    /**
     * was for route to hive, not used any more
     */
    @Deprecated
    public String getHiveUrl() {
        return getOptional("kylin.source.hive.connection-url", "");
    }

    /**
     * was for route to hive, not used any more
     */
    @Deprecated
    public String getHiveUser() {
        return getOptional("kylin.source.hive.connection-user", "");
    }

    /**
     * was for route to hive, not used any more
     */
    @Deprecated
    public String getHivePassword() {
        return getOptional("kylin.source.hive.connection-password", "");
    }

    public String getHiveDatabaseForIntermediateTable() {
        return this.getOptional("kylin.source.hive.database-for-flat-table", "default");
    }

    public String getHiveClientMode() {
        return getOptional("kylin.source.hive.client", "cli");
    }

    public String getHiveBeelineParams() {
        return getOptional("kylin.source.hive.beeline-params", "");
    }

    public int getDefaultVarcharPrecision() {
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-varchar-precision", "4096"));
        if (v < 1) {
            return 4096;
        } else if (v > 65355) {
            return 65535;
        } else {
            return v;
        }
    }

    public int getDefaultCharPrecision() {
        //at most 255 according to https://cwiki.apache.org/confluence/display/Hive/LanguageManual+Types#LanguageManualTypes-CharcharChar
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-char-precision", "255"));
        if (v < 1) {
            return 255;
        } else if (v > 255) {
            return 255;
        } else {
            return v;
        }
    }

    public int getDefaultDecimalPrecision() {
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-decimal-precision", "19"));
        if (v < 1) {
            return 19;
        } else {
            return v;
        }
    }

    public int getDefaultDecimalScale() {
        int v = Integer.parseInt(getOptional("kylin.source.hive.default-decimal-scale", "4"));
        if (v < 1) {
            return 4;
        } else {
            return v;
        }
    }

    // ============================================================================
    // SOURCE.JDBC
    // ============================================================================

    public String getJdbcConnectionUrl() {
        return getOptional("kylin.source.jdbc.connection-url");
    }

    public String getJdbcDriver() {
        return getOptional("kylin.source.jdbc.driver");
    }

    public String getJdbcDialect() {
        return getOptional("kylin.source.jdbc.dialect");
    }

    public String getJdbcUser() {
        return getOptional("kylin.source.jdbc.user");
    }

    public String getJdbcPass() {
        return getOptional("kylin.source.jdbc.pass");
    }

    // ============================================================================
    // STORAGE.PARQUET
    // ============================================================================

    public Map<Integer, String> getStorageEngines() {
        Map<Integer, String> r = Maps.newLinkedHashMap();
        // ref constants in IStorageAware
        r.put(20, "io.kyligence.kap.storage.ParquetDataStorage");
        r.putAll(convertKeyToInteger(getPropertiesByPrefix("kylin.storage.provider.")));
        return r;
    }

    public int getDefaultStorageEngine() {
        return Integer.parseInt(getOptional("kylin.storage.default", "20"));
    }

    public int getDefaultStorageType() {
        return Integer.parseInt(getOptional("kylin.storage.default-storage-type", "0"));
    }

    private static final Pattern JOB_JAR_NAME_PATTERN = Pattern.compile("newten-job(.?)\\.jar");

    private static String getFileName(String homePath, Pattern pattern) {
        File home = new File(homePath);
        SortedSet<String> files = Sets.newTreeSet();
        if (home.exists() && home.isDirectory()) {
            File[] listFiles = home.listFiles();
            if (listFiles != null) {
                for (File file : listFiles) {
                    final Matcher matcher = pattern.matcher(file.getName());
                    if (matcher.matches()) {
                        files.add(file.getAbsolutePath());
                    }
                }
            }
        }
        if (files.isEmpty()) {
            throw new RuntimeException("cannot find " + pattern.toString() + " in " + homePath);
        } else {
            return files.last();
        }
    }

    public String getExtraJarsPath() {
        return getOptional("kylin.engine.extra-jars-path", "");
    }

    public String getKylinJobJarPath() {
        final String jobJar = getOptional("kylin.engine.spark.job-jar");
        if (StringUtils.isNotEmpty(jobJar)) {
            return jobJar;
        }
        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome)) {
            return "";
        }
        return getFileName(kylinHome + File.separator + "lib", JOB_JAR_NAME_PATTERN);
    }

    public String getSparkBuildClassName() {
        return getOptional("kylin.engine.spark.build-class-name", "io.kyligence.kap.engine.spark.job.DFBuildJob");
    }

    public String getSparkTableSamplingClassName() {
        return getOptional("kylin.engine.spark.sampling-class-name",
                "io.kyligence.kap.engine.spark.stats.analyzer.TableAnalyzerJob");
    }

    public String getSparkMergeClassName() {
        return getOptional("kylin.engine.spark.merge-class-name", "io.kyligence.kap.engine.spark.job.DFMergeJob");
    }

    public String getClusterManagerClassName() {
        return getOptional("kylin.engine.spark.cluster-manager-class-name",
                "io.kyligence.kap.cluster.YarnClusterManager");
    }

    public void overrideSparkJobJarPath(String path) {
        System.setProperty("kylin.engine.spark.job-jar", path);
    }

    public int getBuildingCacheThreshold() {
        int threshold = Integer.parseInt(getOptional("kylin.engine.spark.cache-threshold", "100"));
        if (threshold <= 0) {
            threshold = Integer.MAX_VALUE;
        }
        return threshold;
    }

    public Map<String, String> getMRConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.mr.config-override.");
    }

    public Map<String, String> getSparkConfigOverride() {
        return getPropertiesByPrefix("kylin.engine.spark-conf.");
    }

    public int getSparkEngineMaxRetryTime() {
        return Integer.parseInt(getOptional("kylin.engine.max-retry-time", "3"));
    }

    public double getSparkEngineRetryMemoryGradient() {
        return Double.parseDouble(getOptional("kylin.engine.retry-memory-gradient", "1.5"));
    }

    public double getSparkEngineRetryOverheadMemoryGradient() {
        return Double.parseDouble(getOptional("kylin.engine.retry-overheadMemory-gradient", "0.2"));
    }

    public boolean isAutoSetSparkConf() {
        return Boolean.parseBoolean(getOptional("kylin.spark-conf.auto-prior", TRUE));
    }

    public Double getMaxAllocationResourceProportion() {
        return Double.parseDouble(getOptional("kylin.engine.max-allocation-proportion", "0.9"));
    }

    public int getSparkEngineDriverMemoryTableSampling() {
        return Integer.parseInt(getOptional("kylin.engine.driver-memory-table-sampling", "1024"));
    }

    public int getSparkEngineDriverMemoryBase() {
        return Integer.parseInt(getOptional("kylin.engine.driver-memory-base", "1024"));
    }

    public boolean isSanityCheckEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.sanity-check-enabled", TRUE));
    }

    public int[] getSparkEngineDriverMemoryStrategy() {
        String[] dft = { "2", "20", "100" };
        return getOptionalIntArray("kylin.engine.driver-memory-strategy", dft);
    }

    public int getSparkEngineDriverMemoryMaximum() {
        return Integer.parseInt(getOptional("kylin.engine.driver-memory-maximum", "4096"));
    }

    public int getSparkEngineTaskCoreFactor() {
        return Integer.parseInt(getOptional("kylin.engine.spark.task-core-factor", "3"));
    }

    public String getSparkEngineSampleSplitThreshold() {
        return getOptional("kylin.engine.spark.sample-split-threshold", "256m");
    }

    public Boolean getSparkEngineTaskImpactInstanceEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark.task-impact-instance-enabled", TRUE));
    }

    public int getSparkEngineBaseExuctorInstances() {
        return Integer.parseInt(getOptional("kylin.engine.base-executor-instance", "5"));
    }

    public String getSparkEngineExuctorInstanceStrategy() {
        return getOptional("kylin.engine.executor-instance-strategy", "100,2,500,3,1000,4");
    }

    // ============================================================================
    // ENGINE.SPARK
    // ============================================================================

    public String getHadoopConfDir() {
        return getOptional("kylin.env.hadoop-conf-dir", "");
    }

    // ============================================================================
    // QUERY
    // ============================================================================

    public boolean isHeterogeneousSegmentEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.heterogeneous-segment-enabled", TRUE));
    }

    public boolean isUseTableIndexAnswerNonRawQuery() {
        return Boolean.parseBoolean(getOptional("kylin.query.use-tableindex-answer-non-raw-query", FALSE));
    }

    public boolean isTransactionEnabledInQuery() {
        return Boolean.parseBoolean(getOptional("kylin.query.transaction-enable", FALSE));
    }

    public boolean isConvertCreateTableToWith() {
        return Boolean.parseBoolean(getOptional("kylin.query.convert-create-table-to-with", FALSE));
    }

    public boolean isConvertSumExpressionEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.convert-sum-expression-enabled", FALSE));
    }

    /**
     * Rule is usually singleton as static field, the configuration of this property is like:
     * RuleClassName1#FieldName1,RuleClassName2#FieldName2,...
     */
    public List<String> getCalciteAddRule() {
        String rules = getOptional("kylin.query.calcite.add-rule");
        if (StringUtils.isEmpty(rules)) {
            return Lists.newArrayList("io.kyligence.kap.query.optrule.ExtensionOlapJoinRule#INSTANCE");
        }
        return Lists.newArrayList(rules.split(","));
    }

    /**
     * Rule is usually singleton as static field, the configuration of this property is like:
     * RuleClassName1#FieldName1,RuleClassName2#FieldName2,...
     */
    public List<String> getCalciteRemoveRule() {
        String rules = getOptional("kylin.query.calcite.remove-rule");
        if (StringUtils.isEmpty(rules)) {
            return Lists.newArrayList("org.apache.kylin.query.optrule.OLAPJoinRule#INSTANCE");
        }
        return Lists.newArrayList(rules.split(","));
    }

    public boolean isReplaceColCountWithCountStar() {
        return Boolean.valueOf(getOptional("kylin.query.replace-count-column-with-count-star", FALSE));
    }

    // Select star on large table is too slow for BI, add limit by default if missing
    // https://issues.apache.org/jira/browse/KYLIN-2649
    public int getForceLimit() {
        return Integer.parseInt(getOptional("kylin.query.force-limit", "-1"));
    }

    /**
     * the threshold for query result caching
     * query result will only be cached if the result is below the threshold
     * the size of the result is counted by its cells (rows * columns)
     * @return
     */
    public long getLargeQueryThreshold() {
        return Integer.parseInt(getOptional("kylin.query.large-query-threshold", String.valueOf(1000000)));
    }

    public int getLoadCounterCapacity() {
        return Integer.parseInt(getOptional("kylin.query.load-counter-capacity", "50"));
    }

    public long getLoadCounterPeriodSeconds() {
        return TimeUtil.timeStringAs(getOptional("kylin.query.load-counter-period-seconds", "3s"), TimeUnit.SECONDS);
    }

    public Long getCubeBroadcastThreshold() {
        return Long.parseLong(getOptional("kylin.query.cube-broadcast-threshold", String.valueOf(1024L * 1024 * 1024)));
    }

    public String[] getQueryTransformers() {
        String value = getOptional("kylin.query.transformers");
        return value == null ? new String[] { "org.apache.kylin.query.util.PowerBIConverter",
                "org.apache.kylin.query.util.DefaultQueryTransformer", "io.kyligence.kap.query.util.EscapeTransformer",
                "io.kyligence.kap.query.util.ConvertToComputedColumn",
                "org.apache.kylin.query.util.KeywordDefaultDirtyHack", "io.kyligence.kap.query.security.RowFilter" }
                : getOptionalStringArray("kylin.query.transformers", new String[0]);
    }

    public String[] getQueryInterceptors() {
        return getOptionalStringArray("kylin.query.interceptors", new String[0]);
    }

    public long getQueryDurationCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache-threshold-duration", String.valueOf(2000)));
    }

    public long getQueryScanCountCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache-threshold-scan-count", String.valueOf(10 * 1024)));
    }

    public long getQueryScanBytesCacheThreshold() {
        return Long.parseLong(this.getOptional("kylin.query.cache-threshold-scan-bytes", String.valueOf(1024 * 1024)));
    }

    public boolean isQueryCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.cache-enabled", TRUE));
    }

    public boolean isSchemaCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.schema-cache-enabled", FALSE));
    }

    public boolean isQueryIgnoreUnknownFunction() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.ignore-unknown-function", FALSE));
    }

    public boolean isQueryMatchPartialInnerJoinModel() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.match-partial-inner-join-model", FALSE));
    }

    /**
     * if FALSE,
     *     non-equi-inner join will be transformed to inner join and filter
     *     left join will be transformed runtime-join
     * @return
     */
    public boolean isQueryNonEquiJoinModelEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.non-equi-join-model-enabled", FALSE));
    }

    public String getQueryAccessController() {
        return getOptional("kylin.query.access-controller", null);
    }

    public int getDimCountDistinctMaxCardinality() {
        return Integer.parseInt(getOptional("kylin.query.max-dimension-count-distinct", "5000000"));
    }

    public Map<String, String> getUDFs() {
        Map<String, String> udfMap = Maps.newLinkedHashMap();
        udfMap.put("regexp_like", "org.apache.kylin.query.udf.otherUdf.RegexpLikeUDF");
        udfMap.put("rlike", "org.apache.kylin.query.udf.otherUdf.RlikeUDF");
        udfMap.put("if", "org.apache.kylin.query.udf.otherUdf.IfUDF");
        udfMap.put("version", "org.apache.kylin.query.udf.VersionUDF");
        udfMap.put("bitmap_function", "org.apache.kylin.query.udf.IntersectCountByColUDF");
        udfMap.put("concat", "org.apache.kylin.query.udf.stringUdf.ConcatUDF");
        udfMap.put("concat_ws", "org.apache.kylin.query.udf.stringUdf.ConcatwsUDF");
        udfMap.put("massin", "org.apache.kylin.query.udf.MassInUDF");
        udfMap.put("initcapb", "org.apache.kylin.query.udf.stringUdf.InitCapbUDF");
        udfMap.put("substr", "org.apache.kylin.query.udf.stringUdf.SubStrUDF");
        udfMap.put("left", "org.apache.kylin.query.udf.stringUdf.LeftUDF");
        udfMap.put("date_part", "org.apache.kylin.query.udf.dateUdf.DatePartUDF");
        udfMap.put("date_trunc", "org.apache.kylin.query.udf.dateUdf.DateTruncUDF");
        udfMap.put("datediff", "org.apache.kylin.query.udf.dateUdf.DateDiffUDF");
        udfMap.put("unix_timestamp", "org.apache.kylin.query.udf.dateUdf.UnixTimestampUDF");
        udfMap.put("length", "org.apache.kylin.query.udf.stringUdf.LengthUDF");
        udfMap.put("to_timestamp", "org.apache.kylin.query.udf.formatUdf.ToTimestampUDF");
        udfMap.put("to_date", "org.apache.kylin.query.udf.formatUdf.ToDateUDF");
        udfMap.put("to_char", "org.apache.kylin.query.udf.formatUdf.ToCharUDF");
        udfMap.put("date_format", "org.apache.kylin.query.udf.formatUdf.DateFormatUDF");
        udfMap.put("instr", "org.apache.kylin.query.udf.stringUdf.InStrUDF");
        udfMap.put("strpos", "org.apache.kylin.query.udf.stringUdf.StrPosUDF");
        udfMap.put("ifnull", "org.apache.kylin.query.udf.nullHandling.IfNullUDF");
        udfMap.put("isnull", "org.apache.kylin.query.udf.nullHandling.IsNullUDF");
        udfMap.put("split_part", "org.apache.kylin.query.udf.stringUdf.SplitPartUDF");
        udfMap.put("spark_leaf_function", "org.apache.kylin.query.udf.SparkLeafUDF");
        udfMap.put("spark_string_function", "org.apache.kylin.query.udf.SparkStringUDF");
        udfMap.put("spark_misc_function", "org.apache.kylin.query.udf.SparkMiscUDF");
        udfMap.put("spark_time_function", "org.apache.kylin.query.udf.SparkTimeUDF");
        udfMap.put("spark_math_function", "org.apache.kylin.query.udf.SparkMathUDF");
        udfMap.put("spark_other_function", "org.apache.kylin.query.udf.SparkOtherUDF");
        Map<String, String> overrideUdfMap = getPropertiesByPrefix("kylin.query.udf.");
        udfMap.putAll(overrideUdfMap);
        return udfMap;
    }

    public int getSlowQueryDefaultDetectIntervalSeconds() {
        int intervalSec = Integer.parseInt(getOptional("kylin.query.slowquery-detect-interval", "3"));
        if (intervalSec < 1) {
            logger.warn("Slow query detect interval less than 1 sec, set to 1 sec.");
            intervalSec = 1;
        }
        return intervalSec;
    }

    public int getQueryTimeoutSeconds() {
        int time = Integer.parseInt(this.getOptional("kylin.query.timeout-seconds", "300"));
        if (time <= 30) {
            logger.warn("Query timeout seconds less than 30 sec, set to 30 sec.");
            time = 30;
        }
        return time;
    }

    public String getQueryVIPRole() {
        return String.valueOf(getOptional("kylin.query.vip-role", ""));
    }

    public boolean isPushDownEnabled() {
        return Boolean.valueOf(this.getOptional("kylin.query.pushdown-enabled", "true"));
    }

    //see jira KE-11625
    public boolean isPushDownUpdateEnabled() {
        return false;
    }

    public String getPushDownRunnerClassName() {
        return getOptional("kylin.query.pushdown.runner-class-name", "");
    }

    public String getPushDownRunnerClassNameWithDefaultValue() {
        String pushdownRunner = getPushDownRunnerClassName();
        if (StringUtils.isEmpty(pushdownRunner)) {
            pushdownRunner = "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl";
        }
        return pushdownRunner;
    }

    public String[] getPushDownConverterClassNames() {
        return getOptionalStringArray("kylin.query.pushdown.converter-class-names",
                new String[] { "org.apache.kylin.source.adhocquery.DoubleQuotePushDownConverter",
                        "org.apache.kylin.query.util.PowerBIConverter",
                        "io.kyligence.kap.query.util.RestoreFromComputedColumn",
                        "io.kyligence.kap.query.security.RowFilter",
                        "io.kyligence.kap.query.security.HackSelectStarWithColumnACL",
                        "io.kyligence.kap.query.util.SparkSQLFunctionConverter" });
    }

    public String getPartitionCheckRunnerClassName() {
        return getOptional("kylin.query.pushdown.partition-check.runner-class-name", "");
    }

    public String getPartitionCheckRunnerClassNameWithDefaultValue() {
        String partitionCheckRunner = getPartitionCheckRunnerClassName();
        if (StringUtils.isEmpty(partitionCheckRunner)) {
            partitionCheckRunner = "io.kyligence.kap.query.pushdown.PushDownRunnerSparkImpl";
        }
        return partitionCheckRunner;
    }

    public boolean isPushdownQueryCacheEnabled() {
        // KAP#12784 disable all push-down caches, even if the pushdown result is cached, it won't be used
        // Thus this config is set to FALSE by default
        // you may need to change the default value if the pushdown cache issue KAP#13060 is resolved
        return Boolean.parseBoolean(this.getOptional("kylin.query.pushdown.cache-enabled", FALSE));
    }

    public boolean isAutoSetPushDownPartitions() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.pushdown.auto-set-shuffle-partitions-enabled", TRUE));
    }

    public int getBaseShufflePartitionSize() {
        return Integer.parseInt(this.getOptional("kylin.query.pushdown.base-shuffle-partition-size", "48"));
    }

    public String getHiveMetastoreExtraClassPath() {
        return getOptional("kylin.query.pushdown.hive-extra-class-path", "");
    }

    public String getJdbcUrl() {
        return getOptional("kylin.query.pushdown.jdbc.url", "");
    }

    public String getJdbcDriverClass() {
        return getOptional("kylin.query.pushdown.jdbc.driver", "");
    }

    public String getJdbcUsername() {
        return getOptional("kylin.query.pushdown.jdbc.username", "");
    }

    public String getJdbcPassword() {
        return getOptional("kylin.query.pushdown.jdbc.password", "");
    }

    public int getPoolMaxTotal() {
        return Integer.parseInt(this.getOptional("kylin.query.pushdown.jdbc.pool-max-total", "8"));
    }

    public int getPoolMaxIdle() {
        return Integer.parseInt(this.getOptional("kylin.query.pushdown.jdbc.pool-max-idle", "8"));
    }

    public int getPoolMinIdle() {
        return Integer.parseInt(this.getOptional("kylin.query.pushdown.jdbc.pool-min-idle", "0"));
    }

    public boolean isAclTCREnabled() {
        return Boolean.valueOf(this.getOptional("kylin.query.security.acl-tcr-enabled", TRUE));
    }

    public boolean isEscapeDefaultKeywordEnabled() {
        return Boolean.valueOf(this.getOptional("kylin.query.escape-default-keyword", FALSE));
    }

    public String getQueryRealizationFilter() {
        return getOptional("kylin.query.realization-filter", null);
    }

    /**
     * Extras calcite properties to config Calcite connection
     */
    public Map<String, String> getCalciteExtrasProperties() {
        return getPropertiesByPrefix("kylin.query.calcite.extras-props.");
    }

    public boolean isExecuteAsEnabled() {
        return Boolean.valueOf(this.getOptional("kylin.query.query-with-execute-as", FALSE));
    }

    // ============================================================================
    // SERVER
    // ============================================================================

    public String getServerMode() {
        return this.getOptional("kylin.server.mode", "all");
    }

    public boolean isJobNode() {
        return !ClusterConstant.QUERY.equals(getServerMode());
    }

    public boolean isQueryNode() {
        return !ClusterConstant.JOB.equals(getServerMode());
    }

    public boolean isJobNodeOnly() {
        return ClusterConstant.JOB.equals(getServerMode());
    }

    public boolean isQueryNodeOnly() {
        return ClusterConstant.QUERY.equals(getServerMode());
    }

    public boolean isAllNode() {
        return ClusterConstant.ALL.equals(getServerMode());
    }

    public String[] getAllModeServers() {
        return this.getSystemStringArray("kylin.server.cluster-mode-all", new String[0]);
    }

    public String[] getQueryModeServers() {
        return this.getSystemStringArray("kylin.server.cluster-mode-query", new String[0]);
    }

    public String[] getJobModeServers() {
        return this.getSystemStringArray("kylin.server.cluster-mode-job", new String[0]);
    }

    public List<String> getAllServers() {
        List<String> allServers = Lists.newArrayList();

        allServers.addAll(Arrays.asList(getAllModeServers()));
        allServers.addAll(Arrays.asList(getQueryModeServers()));
        allServers.addAll(Arrays.asList(getJobModeServers()));

        return allServers;
    }

    public Boolean getStreamingChangeMeta() {
        return Boolean.parseBoolean(this.getOptional("kylin.server.streaming-change-meta", FALSE));
    }

    public int getServerUserCacheExpireSeconds() {
        return Integer.valueOf(this.getOptional("kylin.server.auth-user-cache.expire-seconds", "300"));
    }

    public int getServerUserCacheMaxEntries() {
        return Integer.valueOf(this.getOptional("kylin.server.auth-user-cache.max-entries", "100"));
    }

    public String getExternalAclProvider() {
        return getOptional("kylin.server.external-acl-provider", "");
    }

    public String getLDAPUserSearchBase() {
        return getOptional("kylin.security.ldap.user-search-base", "");
    }

    public String getLDAPGroupSearchBase() {
        return getOptional("kylin.security.ldap.user-group-search-base", "");
    }

    public String getLDAPAdminRole() {
        return getOptional("kylin.security.acl.admin-role", "");
    }

    // ============================================================================
    // WEB
    // ============================================================================

    public String getTimeZone() {
        String timeZone = getOptional("kylin.web.timezone", "");
        if (StringUtils.isEmpty(timeZone))
            return TimeZone.getDefault().getID();

        return timeZone;
    }

    // ============================================================================
    // RESTCLIENT
    // ============================================================================

    public int getRestClientDefaultMaxPerRoute() {
        return Integer.valueOf(this.getOptional("kylin.restclient.connection.default-max-per-route", "20"));
    }

    public int getRestClientMaxTotal() {
        return Integer.valueOf(this.getOptional("kylin.restclient.connection.max-total", "200"));
    }

    // ============================================================================
    // FAVORITE QUERY
    // ============================================================================
    public int getFavoriteQueryAccelerateThreshold() {
        return Integer.valueOf(this.getOptional("kylin.favorite.query-accelerate-threshold", "20"));
    }

    public boolean getFavoriteQueryAccelerateTipsEnabled() {
        return Boolean.valueOf(this.getOptional("kylin.favorite.query-accelerate-tips-enable", TRUE));
    }

    public int getAutoMarkFavoriteInterval() {
        return Integer.parseInt(this.getOptional("kylin.favorite.auto-mark-detection-interval-minutes", "60")) * 60;
    }

    public int getFavoriteStatisticsCollectionInterval() {
        return Integer.parseInt(this.getOptional("kylin.favorite.statistics-collection-interval-minutes", "60")) * 60;
    }

    public int getFavoriteAccelerateBatchSize() {
        return Integer.valueOf(this.getOptional("kylin.favorite.batch-accelerate-size", "500"));
    }

    public int getFavoriteImportSqlMaxSize() {
        return Integer.valueOf(this.getOptional("kylin.favorite.import-sql-max-size", "1000"));
    }

    // unit of minute
    public long getQueryHistoryScanPeriod() {
        return Long.valueOf(this.getOptional("kylin.favorite.query-history-scan-period-minutes", "60")) * 60 * 1000L;
    }

    // unit of month
    public long getQueryHistoryMaxScanInterval() {
        return Integer.valueOf(this.getOptional("kylin.favorite.query-history-max-scan-interval", "1")) * 30 * 24 * 60
                * 60 * 1000L;
    }

    public int getAutoCheckAccStatusBatchSize() {
        return Integer.parseInt(this.getOptional("kylin.favorite.auto-check-accelerate-batch-size", "100"));
    }

    /**
     * metric
     */
    public String getCodahaleMetricsReportClassesNames() {
        return getOptional("kylin.metrics.reporter-classes", "JsonFileMetricsReporter,JmxMetricsReporter");
    }

    public String getMetricsFileLocation() {
        return getOptional("kylin.metrics.file-location", "/tmp/report.json");
    }

    public Long getMetricsReporterFrequency() {
        return Long.parseLong(getOptional("kylin.metrics.file-frequency", "5000"));
    }

    public String getPerfLoggerClassName() {
        return getOptional("kylin.metrics.perflogger-class", "PerfLogger");
    }

    public boolean isHtraceTracingEveryQuery() {
        return Boolean.valueOf(getOptional("kylin.htrace.trace-every-query", FALSE));
    }

    public String getHdfsWorkingDirectory(String project) {
        if (project != null) {
            return new Path(getHdfsWorkingDirectory(), project).toString() + "/";
        } else {
            return getHdfsWorkingDirectory();
        }
    }

    public String getWorkingDirectoryWithConfiguredFs(String project) {
        String engineWriteFs = getEngineWriteFs();
        if (StringUtils.isEmpty(engineWriteFs)) {
            return getHdfsWorkingDirectory(project);
        }
        // convert scheme from defaultFs to configured Fs
        engineWriteFs = new Path(engineWriteFs, getHdfsWorkingDirectoryWithoutScheme()).toString();
        if (project != null) {
            engineWriteFs = new Path(engineWriteFs, project).toString() + "/";
        }
        return engineWriteFs;
    }

    private String getReadHdfsWorkingDirectory() {
        if (StringUtils.isNotEmpty(getParquetReadFileSystem())) {
            Path workingDir = new Path(getHdfsWorkingDirectory());
            return new Path(getParquetReadFileSystem(), Path.getPathWithoutSchemeAndAuthority(workingDir)).toString()
                    + "/";
        }

        return getHdfsWorkingDirectory();
    }

    public String getReadHdfsWorkingDirectory(String project) {
        if (StringUtils.isNotEmpty(getParquetReadFileSystem())) {
            Path workingDir = new Path(getHdfsWorkingDirectory(project));
            return new Path(getParquetReadFileSystem(), Path.getPathWithoutSchemeAndAuthority(workingDir)).toString()
                    + "/";
        }

        return getHdfsWorkingDirectory(project);
    }

    public String getJdbcHdfsWorkingDirectory() {
        if (StringUtils.isNotEmpty(getJdbcFileSystem())) {
            Path workingDir = new Path(getReadHdfsWorkingDirectory());
            return new Path(getJdbcFileSystem(), Path.getPathWithoutSchemeAndAuthority(workingDir)).toString() + "/";
        }

        return getReadHdfsWorkingDirectory();
    }

    public String getBuildConf() {
        return getOptional("kylin.engine.submit-hadoop-conf-dir", "");
    }

    public String getParquetReadFileSystem() {
        return getOptional("kylin.storage.columnar.file-system", "");
    }

    public String getJdbcFileSystem() {
        return getOptional("kylin.storage.columnar.jdbc-file-system", "");
    }

    public String getPropertiesWhiteList() {
        return getOptional("kylin.web.properties.whitelist",
                "kylin.web.timezone,kylin.env,kylin.security.profile,kylin.source.default,"
                        + "metadata.semi-automatic-mode,kylin.cube.aggrgroup.is-base-cuboid-always-valid,"
                        + "kylin.htrace.show-gui-trace-toggle,kylin.web.export-allow-admin,kylin.web.export-allow-other");
    }

    public Boolean isCalciteInClauseEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite-in-clause-enabled", TRUE));
    }

    public Boolean isCalciteConvertMultipleColumnsIntoOrEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite-convert-multiple-columns-in-to-or-enabled", TRUE));
    }

    public Boolean isEnumerableRulesEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite.enumerable-rules-enabled", FALSE));
    }

    public boolean isReduceExpressionsRulesEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite.reduce-rules-enabled", TRUE));
    }

    public int getEventPollIntervalSecond() {
        return Integer.parseInt(getOptional("kylin.job.event.poll-interval-second", "60"));
    }

    public int getIndexOptimizationLevel() {
        return Integer.parseInt(getOptional("kylin.index.optimization-level", "2"));
    }

    public double getLayoutSimilarityThreshold() {
        return safeParseDouble(getOptional("kylin.index.similarity-ratio-threshold"), SIMILARITY_THRESHOLD);
    }

    public long getSimilarityStrategyRejectThreshold() {
        return safeParseLong(getOptional("kylin.index.beyond-similarity-bias-threshold"), REJECT_SIMILARITY_THRESHOLD);
    }

    public boolean isIncludedStrategyConsiderTableIndex() {
        return Boolean.parseBoolean(getOptional("kylin.index.include-strategy.consider-table-index", TRUE));
    }

    public boolean isLowFreqStrategyConsiderTableIndex() {
        return Boolean.parseBoolean(getOptional("kylin.index.frequency-strategy.consider-table-index", TRUE));
    }

    public long getExecutableSurvivalTimeThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.garbage.storage.executable-survival-time-threshold", "30d"),
                TimeUnit.MILLISECONDS);
    }

    public long getSourceUsageSurvivalTimeThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.garbage.storage.sourceusage-survival-time-threshold", "90d"),
                TimeUnit.MILLISECONDS);
    }

    public long getStorageQuotaSize() {
        return ((Double) (Double.parseDouble(getOptional("kylin.storage.quota-in-giga-bytes", "10240")) * 1024 * 1024
                * 1024)).longValue();
    }

    public long getSourceUsageQuota() {
        Double d = Double.parseDouble(getOptional("kylin.storage.source-quota-in-giga-bytes", "-1"));

        return d >= 0 ? ((Double) (d * 1024 * 1024 * 1024)).longValue() : -1;
    }

    public long getCuboidLayoutSurvivalTimeThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.garbage.storage.cuboid-layout-survival-time-threshold", "7d"),
                TimeUnit.MILLISECONDS);
    }

    public boolean getJobDataLoadEmptyNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-on-empty-data-load", FALSE));
    }

    public boolean getJobErrorNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-on-job-error", FALSE));
    }

    public Long getStorageResourceSurvivalTimeThreshold() {
        return TimeUtil.timeStringAs(this.getOptional("kylin.storage.resource-survival-time-threshold", "7d"),
                TimeUnit.MILLISECONDS);
    }

    public Boolean getTimeMachineEnabled() {
        return Boolean.valueOf(this.getOptional("kylin.storage.time-machine-enabled", FALSE));
    }

    public boolean getJobSourceRecordsChangeNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-on-source-records-change", FALSE));
    }

    public int getMetadataBackupCountThreshold() {
        return Integer.parseInt(getOptional("kylin.metadata.backup-count-threshold", "7"));
    }

    public int getSchedulerLimitPerMinute() {
        return Integer.parseInt(getOptional("kylin.scheduler.schedule-limit-per-minute", "10"));
    }

    public Integer getSchedulerJobTimeOutMinute() {
        return Integer.parseInt(getOptional("kylin.scheduler.schedule-job-timeout-minute", "0"));
    }

    public Long getRateLimitPermitsPerMinute() {
        return Long.valueOf(this.getOptional("kylin.ratelimit.permits-per-minutes", "10"));
    }

    public boolean getSmartModeBrokenModelDeleteEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.broken-model-deleted-on-smart-mode", FALSE));
    }

    public int getPersistFlatTableThreshold() {
        return Integer.parseInt(getOptional("kylin.engine.persist-flattable-threshold", "1"));
    }

    public boolean isPersistFlatTableEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.persist-flattable-enabled", FALSE));
    }

    public boolean isPersistFlatViewEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.persist-flatview", FALSE));
    }

    public boolean isBuildCheckPartitionColEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.check-partition-col-enabled", TRUE));
    }

    public boolean isShardingJoinOptEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.storage.columnar.expose-sharding-trait", TRUE));
    }

    public int getQueryPartitionSplitSizeMB() {
        return Integer.parseInt(getOptional("kylin.storage.columnar.partition-split-size-mb", "64"));
    }

    public String getStorageProvider() {
        return getOptional("kylin.storage.provider", "org.apache.kylin.common.storage.DefaultStorageProvider");
    }

    public String getStreamingBaseCheckpointLocation() {
        return getOptional("kylin.engine.streaming-base-ckeckpoint-location", "/kylin/checkpoint");
    }

    public Boolean getStreamingMetricsEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.engine.streaming-metrics-enabled", FALSE));
    }

    public Integer getStreamingSegmentMergeThresholds() {
        return Integer.parseInt(getOptional("kylin.engine.streaming-segment-merge-threshold", "20"));
    }

    public String getStreamingDuration() {
        return getOptional("kylin.engine.streaming-duration", "30000");
    }

    public Boolean getTriggerOnce() {
        return Boolean.parseBoolean(getOptional("kylin.engine.streaming-trigger-once", FALSE));
    }

    public String getLogSparkDriverPropertiesFile() {
        return getLogPropertyFile("spark-driver-log4j.properties");
    }

    public String getLogSparkExecutorPropertiesFile() {
        return getLogPropertyFile("spark-executor-log4j.properties");
    }

    public String getLogSparkAppMasterPropertiesFile() {
        return getLogPropertyFile("spark-appmaster-log4j.properties");
    }

    private String getLogPropertyFile(String filename) {
        String parentFolder;
        if (isDevEnv()) {
            parentFolder = Paths.get(getKylinHomeWithoutWarn(), "build", "conf").toString();
        } else if (Files.exists(Paths.get(getKylinHomeWithoutWarn(), "conf", filename))) {
            parentFolder = Paths.get(getKylinHomeWithoutWarn(), "conf").toString();
        } else {
            parentFolder = Paths.get(getKylinHomeWithoutWarn(), "server", "conf").toString();
        }
        return parentFolder + File.separator + filename;
    }

    public long getLoadHiveTablenameIntervals() {
        return Long.parseLong(getOptional("kylin.source.load-hive-tablename-interval-seconds", "3600"));
    }

    public boolean getLoadHiveTablenameEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.source.load-hive-tablename-enabled", TRUE));
    }

    //Kerberos
    public boolean getKerberosProjectLevelEnable() {
        return Boolean.parseBoolean(getOptional("kylin.kerberos.project-level-enabled", FALSE));
    }

    private double safeParseDouble(String value, double defaultValue) {
        double result = defaultValue;
        if (StringUtils.isEmpty(value)) {
            return result;
        }
        try {
            result = Double.parseDouble(value.trim());
        } catch (Exception e) {
            logger.error("Detect a malformed double value, set to a default value {}", defaultValue);
        }
        return result;
    }

    private long safeParseLong(String value, long defaultValue) {
        long result = defaultValue;
        if (StringUtils.isEmpty(value)) {
            return result;
        }
        try {
            result = Long.parseLong(value.trim());
        } catch (Exception e) {
            logger.error("Detect a malformed long value, set to a default value {}", defaultValue);
        }
        return result;
    }

    public Boolean isSmartModelEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.env.smart-mode-enabled", FALSE));
    }

    public String getEngineWriteFs() {
        String engineWriteFs = getOptional("kylin.env.engine-write-fs", "");
        return StringUtil.dropSuffix(engineWriteFs, File.separator);
    }

    public boolean isAllowedProjectAdminGrantAcl() {
        String option = getOptional("kylin.security.allow-project-admin-grant-acl", TRUE);
        return !FALSE.equals(option);
    }

    public static File getDiagFileName() {
        String uuid = UUID.randomUUID().toString().toUpperCase().substring(0, 6);
        String packageName = DIAG_ID_PREFIX + new SimpleDateFormat("yyyy_MM_dd_HH_mm_ss").format(new Date()) + "_"
                + uuid;
        String workDir = KylinConfigBase.getKylinHomeWithoutWarn();
        String diagPath = "diag_dump/" + packageName;
        File file;
        if (StringUtils.isNotEmpty(workDir)) {
            file = new File(workDir, diagPath);
        } else {
            file = new File(diagPath);
        }
        return file;
    }

    public boolean isTrackingUrlIpAddressEnabled() {
        return Boolean.valueOf(this.getOptional("kylin.job.tracking-url-ip-address-enabled", TRUE));
    }

    public boolean getEpochCheckerEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.server.leader-race.enabled", "true"));
    }

    public long getEpochExpireTimeSecond() {
        return Long.parseLong(getOptional("kylin.server.leader-race.heart-beat-timeout", "120"));
    }

    public long getEpochCheckerIntervalSecond() {
        return Long.parseLong(getOptional("kylin.server.leader-race.heart-beat-interval", "60"));
    }

    public boolean getJStackDumpTaskEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.task.jstack-dump-enabled", "true"));
    }

    public long getJStackDumpTaskPeriod() {
        return Long.parseLong(getOptional("kylin.task.jstack-dump-interval-minutes", "10"));
    }

    public long getJStackDumpTaskLogsMaxNum() {
        return Math.max(1L, Long.parseLong(getOptional("kylin.task.jstack-dump-log-files-max-count", "20")));
    }

    public int getQueryHistoryMaxSize() {
        return Integer.parseInt(getOptional("kylin.query.queryhistory.max-size", "10000000"));
    }

    public int getQueryHistoryProjectMaxSize() {
        return Integer.parseInt(getOptional("kylin.query.queryhistory.project-max-size", "1000000"));
    }

    public int getQueryHistoryBufferSize() {
        return Integer.parseInt(getOptional("kylin.query.queryhistory.buffer-size", "500"));
    }

    public long getQueryHistorySchedulerInterval() {
        return TimeUtil.timeStringAs(getOptional("kylin.query.queryhistory.scheduler-interval", "3s"),
                TimeUnit.SECONDS);
    }

    public int getQueryHistoryAccelerateBatchSize() {
        return Integer.parseInt(this.getOptional("kylin.favorite.query-history-accelerate-batch-size", "1000"));
    }

    public int getQueryHistoryAccelerateMaxSize() {
        return Integer.parseInt(this.getOptional("kylin.favorite.query-history-accelerate-max-size", "100000"));
    }

    public long getQueryHistoryAccelerateInterval() {
        return TimeUtil.timeStringAs(this.getOptional("kylin.favorite.query-history-accelerate-interval", "60m"),
                TimeUnit.MINUTES);
    }

    public Boolean isSparderAsync() {
        return Boolean.valueOf(this.getOptional("kylin.query.init-sparder-async", TRUE));
    }

    public boolean getRandomAdminPasswordEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.random-admin-password.enabled", TRUE));
    }

    public long getCatchUpInterval() {
        return TimeUtil.timeStringAs(getOptional("kylin.metadata.audit-log.catchup-interval", "5s"), TimeUnit.SECONDS);
    }

    public long getCatchUpTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.metadata.audit-log.catchup-timeout", "2s"), TimeUnit.SECONDS);
    }

    public long getUpdateEpochTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.server.leader-race.update-heart-beat-timeout", "30s"),
                TimeUnit.SECONDS);
    }

    public boolean isQueryEscapedLiteral() {
        return Boolean.parseBoolean(getOptional("kylin.query.parser.escaped-string-literals", FALSE));
    }

    public boolean isSessionSecureRandomCreateEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.web.session.secure-random-create-enabled", FALSE));
    }

    public boolean isSessionJdbcEncodeEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.web.session.jdbc-encode-enabled", FALSE));
    }

    public String getSpringStoreType() {
        return getOptional("spring.session.store-type", "");
    }

    public int getCapacitySampleRows() {
        return Integer.parseInt(getOptional("kylin.capacity.sample-rows", "1000"));
    }

    public String getUserPasswordEncoder() {
        return getOptional("kylin.security.user-password-encoder",
                "org.springframework.security.crypto.bcrypt.BCryptPasswordEncoder");
    }

    public int getRecommendationPageSize() {
        return Integer.parseInt(getOptional("kylin.model.recommendation-page-size", "500"));
    }

    // Guardian Process
    public boolean isGuardianEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.guardian.enabled", FALSE));
    }

    private long getConfigItemSeconds(String configItem, long defaultLongValue, long rangeStart, long rangeEnd) {
        long resultValue = defaultLongValue;
        try {
            resultValue = TimeUtil.timeStringAs(getOptional(configItem, String.format("%dS", defaultLongValue)),
                    TimeUnit.SECONDS);
        } catch (Exception e) {
            return resultValue;
        }

        return rangeStart <= resultValue && resultValue <= rangeEnd ? resultValue : defaultLongValue;
    }

    private int getConfigItemIntValue(String configItem, int defaultIntValue, int rangeStart, int rangeEnd) {
        int resultValue = defaultIntValue;
        try {
            resultValue = Integer.parseInt(getOptional(configItem, String.valueOf(defaultIntValue)));
        } catch (Exception e) {
            return resultValue;
        }

        return rangeStart <= resultValue && resultValue <= rangeEnd ? resultValue : defaultIntValue;
    }

    private double getConfigItemDoubleValue(String configItem, double defaultDoubleValue, double rangeStart,
            double rangeEnd) {
        double resultValue = defaultDoubleValue;
        try {
            resultValue = Integer.parseInt(getOptional(configItem, String.valueOf(defaultDoubleValue)));
        } catch (Exception e) {
            return resultValue;
        }

        return rangeStart <= resultValue && resultValue <= rangeEnd ? resultValue : defaultDoubleValue;
    }

    public long getGuardianCheckInterval() {
        return getConfigItemSeconds("kylin.guardian.check-interval", MINUTE, MINUTE, 60 * MINUTE);
    }

    public long getGuardianCheckInitDelay() {
        return getConfigItemSeconds("kylin.guardian.check-init-delay", 5 * MINUTE, MINUTE, 60 * MINUTE);
    }

    public boolean isGuardianHAEnabled() {
        return !FALSE.equalsIgnoreCase(getOptional("kylin.guardian.ha-enabled", TRUE));
    }

    public long getGuardianHACheckInterval() {
        return getConfigItemSeconds("kylin.guardian.ha-check-interval", MINUTE, MINUTE, 60 * MINUTE);
    }

    public long getGuardianHACheckInitDelay() {
        return getConfigItemSeconds("kylin.guardian.ha-check-init-delay", 5 * MINUTE, MINUTE, 60 * MINUTE);
    }

    public String getGuardianHealthCheckers() {
        return getOptional("kylin.guardian.checkers",
                "io.kyligence.kap.tool.daemon.checker.KEProcessChecker,io.kyligence.kap.tool.daemon.checker.FullGCDurationChecker,io.kyligence.kap.tool.daemon.checker.KEStatusChecker");
    }

    public int getGuardianFullGCCheckFactor() {
        return getConfigItemIntValue("kylin.guardian.full-gc-check-factor", 5, 1, 60);
    }

    public boolean isFullGCRatioBeyondRestartEnabled() {
        return !FALSE.equalsIgnoreCase(getOptional("kylin.guardian.full-gc-duration-ratio-restart-enabled", "true"));
    }

    public double getGuardianFullGCRatioThreshold() {
        return getConfigItemDoubleValue("kylin.guardian.full-gc-duration-ratio-threshold", 75.0, 0.0, 100.0);
    }

    public boolean isDowngradeOnFullGCBusyEnable() {
        return !FALSE.equalsIgnoreCase(getOptional("kylin.guardian.downgrade-on-full-gc-busy-enabled", "true"));
    }

    public double getGuardianFullGCHighWatermark() {
        return getConfigItemDoubleValue("kylin.guardian.full-gc-busy-high-watermark", 40.0, 0.0, 100.0);
    }

    public double getGuardianFullGCLowWatermark() {
        return getConfigItemDoubleValue("kylin.guardian.full-gc-busy-low-watermark", 20.0, 0.0, 100.0);
    }

    public int getGuardianApiFailThreshold() {
        return getConfigItemIntValue("kylin.guardian.api-fail-threshold", 5, 1, 100);
    }

    public boolean isSparkFailRestartKeEnabled() {
        return !FALSE.equalsIgnoreCase(getOptional("kylin.guardian.restart-spark-fail-restart-enabled", "true"));
    }

    public int getGuardianSparkFailThreshold() {
        return getConfigItemIntValue("kylin.guardian.restart-spark-fail-threshold", 3, 1, 100);
    }

    public int getDowngradeParallelQueryThreshold() {
        return getConfigItemIntValue("kylin.guardian.downgrade-mode-parallel-query-threshold", 10, 0, 100);
    }

    public boolean isSlowQueryKillFailedRestartKeEnabled() {
        return !FALSE.equalsIgnoreCase(getOptional("kylin.guardian.kill-slow-query-fail-restart-enabled", "true"));
    }

    public Integer getGuardianSlowQueryKillFailedThreshold() {
        return getConfigItemIntValue("kylin.guardian.kill-slow-query-fail-threshold", 3, 1, 100);
    }

    public Long getLightningClusterId() {
        return Long.parseLong(getOptional("kylin.lightning.cluster-id", "0"));
    }

    public String getLightningServerZkNode() {
        return getOptional("kylin.lightning.server.zookeeper-node", "/kylin/management");
    }

    public String getSparkLogExtractor() {
        return getOptional("kylin.tool.spark-log-extractor", "io.kyligence.kap.tool.YarnSparkLogExtractor");
    }

    public String getMountSparkLogDir() {
        return getOptional("kylin.tool.mount-spark-log-dir", "");
    }

    public boolean cleanDiagTmpFile() {
        return Boolean.parseBoolean(getOptional("kylin.tool.clean-diag-tmp-file", FALSE));
    }

    public int getTurnMaintainModeRetryTimes() {
        return Integer.parseInt(getOptional("kylin.tool.turn-on-maintainmodel-retry-times", "3"));
    }

    public int getSuggestModelSqlLimit() {
        return Integer.parseInt(getOptional("kylin.model.suggest-model-sql-limit", "200"));
    }

    public int getSuggestModelSqlInterval() {
        return Integer.parseInt(getOptional("kylin.model.suggest-model-sql-interval", "10"));
    }

    public String getIntersectFilterOrSeparator() {
        return getOptional("kylin.query.intersect.separator", "|");
    }

    public int getBitmapValuesUpperBound() {
        return Integer.parseInt(getOptional("kylin.query.bitmap-values-upper-bound", "10000000"));
    }

    public String getUIProxyLocation() {
        return getOptional("kylin.query.ui.proxy-location", KYLIN_ROOT);
    }

    public String getJobFinishedNotifierUrl() {
        return getOptional("kylin.job.finished-notifier-url", null);
    }

    public int getMaxModelDimensionMeasureNameLength() {
        return Integer.parseInt(getOptional("kylin.model.dimension-measure-name.max-length", "300"));
    }

    public int getAuditLogBatchSize() {
        return Integer.parseInt(getOptional("kylin.metadata.audit-log.batch-size", "5000"));
    }

    public long getDiagTaskTimeout() {
        return TimeUtil.timeStringAs(getOptional("kylin.diag.task-timeout", "180s"), TimeUnit.SECONDS);
    }

    public ImmutableSet<String> getDiagTaskTimeoutBlackList() {
        String lists = getOptional("kylin.diag.task-timeout-black-list", "METADATA,LOG").toUpperCase();
        return ImmutableSet.copyOf(lists.split(","));
    }

    public boolean isMetadataOnlyForRead() {
        return Boolean.valueOf(getOptional("kylin.env.metadata.only-for-read", FALSE));
    }
}
