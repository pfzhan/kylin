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

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.SortedSet;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.text.StrSubstitutor;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.lock.DistributedLockFactory;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.TimeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.metadata.HDFSMetadataStore;

/**
 * An abstract class to encapsulate access to a set of 'properties'.
 * Subclass can override methods in this class to extend the content of the 'properties',
 * with some override values for example.
 */
public abstract class KylinConfigBase implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KylinConfigBase.class);

    private static final String WORKING_DIR_PROP = "kylin.env.hdfs-working-dir";
    private static final String KYLIN_ROOT = "/kylin";

    public static final long REJECT_SIMILARITY_THRESHOLD = 100_000_000L;
    public static final double SIMILARITY_THRESHOLD = 0.9;

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
            logger.info("SPARK_HOME was set to {}", sparkHome);
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

    final protected Map<String, String> getPropertiesByPrefix(String prefix) {
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

    final protected String[] getOptionalStringArray(String prop, String[] dft) {
        final String property = getOptional(prop);
        if (!StringUtils.isBlank(property)) {
            return property.split("\\s*,\\s*");
        } else {
            return dft;
        }
    }

    final protected int[] getOptionalIntArray(String prop, String[] dft) {
        String[] strArray = getOptionalStringArray(prop, dft);
        int[] intArray = new int[strArray.length];
        for (int i = 0; i < strArray.length; i++) {
            intArray[i] = Integer.parseInt(strArray[i]);
        }
        return intArray;
    }

    final protected String getRequired(String prop) {
        String r = getOptional(prop);
        if (StringUtils.isEmpty(r)) {
            throw new IllegalArgumentException("missing '" + prop + "' in conf/kylin.properties");
        }
        return r;
    }

    /**
     * Use with care, properties should be read-only. This is for testing only.
     */
    final public void setProperty(String key, String value) {
        logger.trace("KylinConfig was updated with " + key + "=" + value);
        properties.setProperty(BCC.check(key), value);
    }

    final protected void reloadKylinConfig(Properties properties) {
        this.properties = BCC.check(properties);
        setProperty("kylin.metadata.url.identifier", getMetadataUrlPrefix());
        setProperty("kylin.log.spark-executor-properties-file", getLogSparkExecutorPropertiesFile());
        setProperty("kylin.log.spark-driver-properties-file", getLogSparkDriverPropertiesFile());
        setProperty("kylin.log.spark-appmaster-properties-file", getLogSparkAppMasterPropertiesFile());

        // https://github.com/kyligence/kap/issues/12654
        this.properties.put(WORKING_DIR_PROP,
                makeQualified(new Path(this.properties.getProperty(WORKING_DIR_PROP, KYLIN_ROOT))).toString());
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
        return getOptional("kylin.env", "DEV");
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

        String root = getOptional(WORKING_DIR_PROP, KYLIN_ROOT);

        Path path = new Path(root);
        if (!path.isAbsolute())
            throw new IllegalArgumentException("kylin.env.hdfs-working-dir must be absolute, but got " + root);

        // make sure path is qualified
        path = makeQualified(path);

        // append metadata-url prefix
        String metaId = getMetadataUrlPrefix().replace(':', '-').replace('/', '-');
        root = new Path(path, metaId).toString();

        if (!root.endsWith("/"))
            root += "/";

        cachedHdfsWorkingDirectory = root;
        if (cachedHdfsWorkingDirectory.startsWith("file:")) {
            cachedHdfsWorkingDirectory = cachedHdfsWorkingDirectory.replace("file:", "file://");
        } else if (cachedHdfsWorkingDirectory.startsWith("maprfs:")) {
            cachedHdfsWorkingDirectory = cachedHdfsWorkingDirectory.replace("maprfs:", "maprfs://");
        }
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
        return Boolean.parseBoolean(getOptional("kylin.metrics.monitor-enabled", "false"));
    }

    public String getZookeeperBasePath() {
        return getOptional("kylin.env.zookeeper-base-path", KYLIN_ROOT);
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
        return Boolean.parseBoolean(getOptional("kylin.env.zookeeper-acl-enabled", "false"));
    }

    public String getZKAuths() {
        return getOptional("kylin.env.zookeeper.zk-auth", "digest:ADMIN:KYLIN");
    }

    public String getZKAcls() {
        return getOptional("kylin.env.zookeeper.zk-acl", "world:anyone:rwcda");
    }

    public String getYarnStatusCheckUrl() {
        return getOptional("kylin.job.yarn.app.rest.check.status.url", null);
    }

    // ============================================================================
    // METADATA
    // ============================================================================

    public int getQueryConcurrentRunningThresholdForProject() {
        // by default there's no limitation
        return Integer.parseInt(getOptional("kylin.query.project-concurrent-running-threshold", "0"));
    }

    public boolean isAdminUserExportAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.web.export-allow-admin", "true"));
    }

    public boolean isNoneAdminUserExportAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.web.export-allow-other", "true"));
    }

    public StorageURL getMetadataUrl() {
        return StorageURL.valueOf(getOptional("kylin.metadata.url", "kylin_metadata@jdbc"));
    }

    public boolean isMetadataAuditLogEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.audit-log.enabled", "true"));
    }

    public long getMetadataAuditLogMaxSize() {
        return Long.valueOf(getOptional("kylin.metadata.audit-log.max-size", "3000000"));
    }

    public StorageURL getMetadataMQUrl() {
        return StorageURL.valueOf(getOptional("kylin.metadata.mq-url", ""));
    }

    public int getCacheSyncRetrys() {
        return Integer.parseInt(getOptional("kylin.metadata.sync-retries", "3"));
    }

    // for test only
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

    public DistributedLockFactory getDistributedLockFactory() {
        String clsName = getOptional("kylin.metadata.distributed-lock-impl",
                "org.apache.kylin.job.lock.ZookeeperDistributedLock$Factory");
        return (DistributedLockFactory) ClassUtil.newInstance(clsName);
    }

    public boolean isCheckCopyOnWrite() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.check-copy-on-write", "false"));
    }

    public String getServerPort() {
        return getOptional("server.port", "7070");
    }

    public boolean isServerHttpsEnabled() {
        return Boolean.valueOf(getOptional("kylin.server.https.enable", "false"));
    }

    public int getServerHttpsPort() {
        return Integer.parseInt(getOptional("kylin.server.https.port", "7443"));
    }

    public String getServerHttpsKeyType() {
        return getOptional("kylin.server.https.keystore.type", "JKS");
    }

    public String getServerHttpsKeystore() {
        return getOptional("kylin.server.https.keystore.file", getKylinHome() + "/server/.keystore");
    }

    public String getServerHttpsKeyPassword() {
        return getOptional("kylin.server.https.keystore.password", "changeit");
    }

    public String getServerHttpsKeyAlias() {
        return getOptional("kylin.server.https.key.alias", null);
    }

    public boolean isSemiAutoMode() {
        return Boolean.parseBoolean(getOptional("kap.metadata.semi-automatic-mode", "false"));
    }

    // ============================================================================
    // DICTIONARY & SNAPSHOT
    // ============================================================================

    public boolean isUseForestTrieDictionary() {
        return Boolean.parseBoolean(getOptional("kylin.dictionary.use-forest-trie", "true"));
    }

    public int getTrieDictionaryForestMaxTrieSizeMB() {
        return Integer.parseInt(getOptional("kylin.dictionary.forest-trie-max-mb", "500"));
    }

    public int getCachedDictMaxEntrySize() {
        return Integer.parseInt(getOptional("kylin.dictionary.max-cache-entry", "3000"));
    }

    public boolean isGrowingDictEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.dictionary.growing-enabled", "false"));
    }

    public int getAppendDictHashPartitions() {
        return Integer.parseInt(getOptional("kylin.dictionary.append-hash-partitions", "100"));
    }

    public int getAppendDictEntrySize() {
        return Integer.parseInt(getOptional("kylin.dictionary.append-entry-size", "10000000"));
    }

    public int getAppendDictMaxVersions() {
        return Integer.parseInt(getOptional("kylin.dictionary.append-max-versions", "3"));
    }

    public int getAppendDictVersionTTL() {
        return Integer.parseInt(getOptional("kylin.dictionary.append-version-ttl", "259200000"));
    }

    public boolean isSnapshotParallelBuildEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.snapshot.parallel-build-enabled", "true"));
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
        return Integer.parseInt(getOptional("kylin.dictionary.globalV2-min-hash-partitions", "10"));
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

    // ============================================================================
    // CUBE
    // ============================================================================

    public String getSegmentAdvisor() {
        return getOptional("kylin.cube.segment-advisor", "org.apache.kylin.cube.CubeSegmentAdvisor");
    }

    @Deprecated
    public double getJobCuboidSizeMemHungryRatio() {
        return Double.parseDouble(getOptional("kylin.cube.size-estimate-memhungry-ratio", "0.05"));
    }

    public long getCubeAggrGroupMaxCombination() {
        return Long.parseLong(getOptional("kylin.cube.aggrgroup.max-combination", "4096"));
    }

    public boolean getCubeAggrGroupIsMandatoryOnlyValid() {
        return Boolean.parseBoolean(getOptional("kylin.cube.aggrgroup.is-mandatory-only-valid", "true"));
    }

    public int getLowFrequencyThreshold() {
        return Integer.parseInt(this.getOptional("kylin.cube.low-frequency-threshold", "5"));
    }

    public int getFrequencyTimeWindowInDays() {
        return Integer.parseInt(this.getOptional("kylin.cube.frequency-time-window", "30"));
    }

    public boolean isBaseCuboidAlwaysValid() {
        return Boolean.parseBoolean(this.getOptional("kylin.cube.aggrgroup.is-base-cuboid-always-valid", "true"));
    }

    // ============================================================================
    // Cube Planner
    // ============================================================================

    public boolean isCubePlannerEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.cube.cubeplanner.enabled", "false"));
    }

    public boolean isCubePlannerEnabledForExistingCube() {
        return Boolean.parseBoolean(getOptional("kylin.cube.cubeplanner.enabled-for-existing-cube", "false"));
    }

    public double getCubePlannerExpansionRateThreshold() {
        return Double.parseDouble(getOptional("kylin.cube.cubeplanner.expansion-threshold", "15.0"));
    }

    public int getCubePlannerRecommendCuboidCacheMaxSize() {
        return Integer.parseInt(getOptional("kylin.cube.cubeplanner.recommend-cache-max-size", "200"));
    }

    public long getCubePlannerMandatoryRollUpThreshold() {
        return Long.parseLong(getOptional("kylin.cube.cubeplanner.mandatory-rollup-threshold", "1000"));
    }

    public int getCubePlannerAgreedyAlgorithmAutoThreshold() {
        return Integer.parseInt(getOptional("kylin.cube.cubeplanner.algorithm-threshold-greedy", "10"));
    }

    public int getCubePlannerGeneticAlgorithmAutoThreshold() {
        return Integer.parseInt(getOptional("kylin.cube.cubeplanner.algorithm-threshold-genetic", "23"));
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

    public String getKylinJobLogDir() {
        return getOptional("kylin.job.log-dir", "/tmp/kylin/logs");
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

    public String getCliWorkingDir() {
        return getOptional("kylin.job.remote-cli-working-dir");
    }

    public boolean isEmptySegmentAllowed() {
        return Boolean.parseBoolean(getOptional("kylin.job.allow-empty-segment", "true"));
    }

    public int getMaxConcurrentJobLimit() {
        return Integer.parseInt(getOptional("kylin.job.max-concurrent-jobs", "10"));
    }

    public Boolean getAutoSetConcurrentJob() {
        if (isDevOrUT()) {
            return Boolean.parseBoolean(getOptional("kylin.job.auto-set-concurrent-jobs", "false"));
        }
        return Boolean.parseBoolean(getOptional("kylin.job.auto-set-concurrent-jobs", "true"));
    }

    public double getMaxLocalConsumptionRatio() {
        return Double.parseDouble(getOptional("kylin.job.max-local-consumption-ratio", "0.5"));
    }

    public int getCubingInMemSamplingPercent() {
        int percent = Integer.parseInt(this.getOptional("kylin.job.sampling-percentage", "100"));
        percent = Math.max(percent, 1);
        percent = Math.min(percent, 100);
        return percent;
    }

    public String getHiveDependencyFilterList() {
        return this.getOptional("kylin.job.dependency-filter-list", "[^,]*hive-exec[^,]*?\\.jar" + "|"
                + "[^,]*hive-metastore[^,]*?\\.jar" + "|" + "[^,]*hive-hcatalog-core[^,]*?\\.jar");
    }

    public boolean isMailEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-enabled", "false"));
    }

    public boolean isStarttlsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-mail-enable-starttls", "false"));
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

    public int getCubeStatsHLLPrecision() {
        return Integer.parseInt(getOptional("kylin.job.sampling-hll-precision", "14"));
    }

    public String getJobControllerLock() {
        return getOptional("kylin.job.lock", "org.apache.kylin.storage.hbase.util.ZookeeperJobLock");
    }

    public Integer getSchedulerPollIntervalSecond() {
        return Integer.parseInt(getOptional("kylin.job.scheduler.poll-interval-second", "30"));
    }

    public Integer getErrorRecordThreshold() {
        return Integer.parseInt(getOptional("kylin.job.error-record-threshold", "0"));
    }

    public boolean isAdvancedFlatTableUsed() {
        return Boolean.parseBoolean(getOptional("kylin.job.use-advanced-flat-table", "false"));
    }

    public String getAdvancedFlatTableClass() {
        return getOptional("kylin.job.advanced-flat-table.class");
    }

    public boolean isFlatTableJoinWithoutLookup() {
        return Boolean.parseBoolean(getOptional("kylin.job.flat-table-join-without-lookup", "false"));
    }

    public String getJobTrackingURLPattern() {
        return getOptional("kylin.job.tracking-url-pattern", "");
    }

    public boolean isJobLogPrintEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.log-print-enabled", "true"));

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

    public Map<String, String> getHiveConfigOverride() {
        return getPropertiesByPrefix("kylin.source.hive.config-override.");
    }

    public String getOverrideHiveTableLocation(String table) {
        return getOptional("kylin.source.hive.table-location." + table.toUpperCase());
    }

    public boolean isHiveKeepFlatTable() {
        return Boolean.parseBoolean(this.getOptional("kylin.source.hive.keep-flat-table", "false"));
    }

    public String getHiveDatabaseForIntermediateTable() {
        return this.getOptional("kylin.source.hive.database-for-flat-table", "default");
    }

    public boolean isHiveRedistributeEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.source.hive.redistribute-flat-table", "true"));
    }

    public String getHiveClientMode() {
        return getOptional("kylin.source.hive.client", "cli");
    }

    public String getHiveBeelineParams() {
        return getOptional("kylin.source.hive.beeline-params", "");
    }

    public String getFlatHiveTableClusterByDictColumn() {
        return getOptional("kylin.source.hive.flat-table-cluster-by-dict-column");
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
    // SOURCE.KAFKA
    // ============================================================================

    public Map<String, String> getKafkaConfigOverride() {
        return getPropertiesByPrefix("kylin.source.kafka.config-override.");
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

    public String getSqoopHome() {
        return getOptional("kylin.source.jdbc.sqoop-home");
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

    public String getClusterInfoFetcherClassName() {
        return getOptional("kylin.engine.spark.cluster-info-fetcher-class-name",
                "io.kyligence.kap.cluster.YarnInfoFetcher");
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

    public String getKylinJobMRLibDir() {
        return getOptional("kylin.engine.mr.lib-dir", "");
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
        return Boolean.parseBoolean(getOptional("kylin.spark-conf.auto.prior", "true"));
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
        return Boolean.parseBoolean(getOptional("kylin.engine.spark.task-impact-instance-enabled", "true"));
    }

    public Boolean getSparkEngineDataImpactInstanceEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark.data-impact-instance-enabled", "true"));
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

    public String getSparkAdditionalJars() {
        return getOptional("kylin.engine.spark.additional-jars", "");
    }

    public float getSparkRDDPartitionCutMB() {
        return Float.valueOf(getOptional("kylin.engine.spark.rdd-partition-cut-mb", "10.0"));
    }

    public int getSparkMinPartition() {
        return Integer.valueOf(getOptional("kylin.engine.spark.min-partition", "1"));
    }

    public int getSparkMaxPartition() {
        return Integer.valueOf(getOptional("kylin.engine.spark.max-partition", "5000"));
    }

    public boolean isSparkSanityCheckEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.engine.spark.sanity-check-enabled", "false"));
    }

    // ============================================================================
    // QUERY
    // ============================================================================

    public boolean isUseTableIndexAnswerNonRawQuery() {
        return Boolean.valueOf(getOptional("kylin.query.use-tableindex-answer-non-raw-query", "false"));
    }

    public boolean isTransactionEnabledInQuery() {
        return Boolean.valueOf(getOptional("kylin.query.transaction-enable", "false"));
    }

    public boolean isConvertCreateTableToWith() {
        return Boolean.valueOf(getOptional("kylin.query.convert-create-table-to-with", "false"));
    }

    /**
     * Rule is usually singleton as static field, the configuration of this property is like:
     * RuleClassName1#FieldName1,RuleClassName2#FieldName2,...
     */
    public List<String> getCalciteAddRule() {
        String rules = getOptional("kylin.query.calcite.add-rule");
        if (rules == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(rules.split(","));
    }

    /**
     * Rule is usually singleton as static field, the configuration of this property is like:
     * RuleClassName1#FieldName1,RuleClassName2#FieldName2,...
     */
    public List<String> getCalciteRemoveRule() {
        String rules = getOptional("kylin.query.calcite.remove-rule");
        if (rules == null) {
            return Lists.newArrayList();
        }
        return Lists.newArrayList(rules.split(","));
    }

    public boolean isReplaceColCountWithCountStar() {
        return Boolean.valueOf(getOptional("kylin.query.replace-count-column-with-count-star", "false"));
    }

    //check KYLIN-1684, in most cases keep the default value
    public boolean isSkippingEmptySegments() {
        return Boolean.valueOf(getOptional("kylin.query.skip-empty-segments", "true"));
    }

    public boolean isDisableCubeNoAggSQL() {
        return Boolean.valueOf(getOptional("kylin.query.disable-cube-noagg-sql", "false"));
    }

    public boolean isStreamAggregateEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.stream-aggregate-enabled", "true"));
    }

    @Deprecated //Limit is good even it's large. This config is meaning less since we already have scan threshold
    public int getStoragePushDownLimitMax() {
        return Integer.parseInt(getOptional("kylin.query.max-limit-pushdown", "10000"));
    }

    // Select star on large table is too slow for BI, add limit by default if missing
    // https://issues.apache.org/jira/browse/KYLIN-2649
    public int getForceLimit() {
        return Integer.parseInt(getOptional("kylin.query.force-limit", "-1"));
    }

    @Deprecated
    public int getScanThreshold() {
        return Integer.parseInt(getOptional("kylin.query.scan-threshold", "10000000"));
    }

    public long getQueryMaxScanBytes() {
        long value = Long.parseLong(getOptional("kylin.query.max-scan-bytes", "0"));
        return value > 0 ? value : Long.MAX_VALUE;
    }

    public int getTranslatedInClauseMaxSize() {
        return Integer.parseInt(getOptional("kylin.query.translated-in-clause-max-size", String.valueOf(1024 * 1024)));
    }

    public int getLargeQueryThreshold() {
        return Integer.parseInt(getOptional("kylin.query.large-query-threshold", String.valueOf(1000000)));
    }

    public int getDerivedInThreshold() {
        return Integer.parseInt(getOptional("kylin.query.derived-filter-translation-threshold", "20"));
    }

    public String[] getQueryTransformers() {
        return getOptionalStringArray("kylin.query.transformers", new String[0]);
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

    public boolean isQuerySecureEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.security-enabled", "true"));
    }

    public boolean isQueryCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.cache-enabled", "true"));
    }

    public boolean isQueryIgnoreUnknownFunction() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.ignore-unknown-function", "false"));
    }

    public boolean isQueryMatchPartialInnerJoinModel() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.match-partial-inner-join-model", "false"));
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
        udfMap.put("concat", "org.apache.kylin.query.udf.stringUdf.ConcatUDF");
        udfMap.put("massin", "org.apache.kylin.query.udf.MassInUDF");
        udfMap.put("initcapb", "org.apache.kylin.query.udf.stringUdf.InitCapbUDF");
        udfMap.put("substr", "org.apache.kylin.query.udf.stringUdf.SubStrUDF");
        udfMap.put("left", "org.apache.kylin.query.udf.stringUdf.LeftUDF");
        udfMap.put("add_months", "org.apache.kylin.query.udf.dateUdf.AddMonthsUDF");
        udfMap.put("date_part", "org.apache.kylin.query.udf.dateUdf.DatePartUDF");
        udfMap.put("date_trunc", "org.apache.kylin.query.udf.dateUdf.DateTruncUDF");
        udfMap.put("datediff", "org.apache.kylin.query.udf.dateUdf.DateDiffUDF");
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
        return String.valueOf(getOptional("kap.query.vip-role", ""));
    }

    public boolean isPushDownEnabled() {
        return StringUtils.isNotEmpty(getPushDownRunnerClassName());
    }

    public boolean isPushDownUpdateEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.pushdown.update-enabled", "false"));
    }

    public String getSchemaFactory() {
        return this.getOptional("kylin.query.schema-factory", "io.kyligence.kap.query.schema.KapSchemaFactory");
    }

    public String getPushDownRunnerClassName() {
        return getOptional("kylin.query.pushdown.runner-class-name", "");
    }

    public String[] getPushDownConverterClassNames() {
        return getOptionalStringArray("kylin.query.pushdown.converter-class-names",
                new String[] { "org.apache.kylin.source.adhocquery.HivePushDownConverter" });
    }

    public boolean isPushdownQueryCacheEnabled() {
        return Boolean.parseBoolean(this.getOptional("kylin.query.pushdown.cache-enabled", "true"));
    }

    public boolean isAutoSetPushDownPartitions() {
        return Boolean
                .parseBoolean(this.getOptional("kylin.query.pushdown.auto-set-shuffle-partitions-enabled", "true"));
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
        return Boolean.valueOf(this.getOptional("kylin.query.security.acl-tcr-enabled", "true"));
    }

    public boolean isTableACLEnabled() {
        return Boolean.valueOf(this.getOptional("kylin.query.security.table-acl-enabled", "true"));
    }

    public boolean isEscapeDefaultKeywordEnabled() {
        return Boolean.valueOf(this.getOptional("kylin.query.escape-default-keyword", "false"));
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

    // ============================================================================
    // SERVER
    // ============================================================================

    public String getServerMode() {
        return this.getOptional("kylin.server.mode", "all");
    }

    public String[] getRestServers() {
        return getOptionalStringArray("kylin.server.cluster-servers", new String[] { "admin:KYLIN@localhost:7070" });
    }

    public String getClusterName() {
        return this.getOptional("kylin.server.cluster-name", getMetadataUrlPrefix());
    }

    public String getNodeId() {
        return getOptional("kylin.server.node-id", "kylin0");
    }

    public int getWorkersPerServer() {
        //for sequence sql use
        return Integer.parseInt(getOptional("kylin.server.sequence-sql.workers-per-server", "1"));
    }

    public long getSequenceExpireTime() {
        return Long.valueOf(this.getOptional("kylin.server.sequence-sql.expire-time", "86400000"));//default a day
    }

    public boolean getQueryMetricsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.server.query-metrics-enabled", "false"));
    }

    public boolean getQueryMetrics2Enabled() {
        return Boolean.parseBoolean(getOptional("kylin.server.query-metrics2-enabled", "false"));
    }

    public int[] getQueryMetricsPercentilesIntervals() {
        String[] dft = { "60", "300", "3600" };
        return getOptionalIntArray("kylin.server.query-metrics-percentiles-intervals", dft);
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

    public boolean isWebCrossDomainEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.web.cross-domain-enabled", "true"));
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
        return Boolean.valueOf(this.getOptional("kylin.favorite.query-accelerate-tips-enable", "true"));
    }

    public int getAutoMarkFavoriteInterval() {
        return Integer.parseInt(this.getOptional("kylin.favorite.auto-mark-detection-interval.minutes", "60")) * 60;
    }

    public int getFavoriteStatisticsCollectionInterval() {
        return Integer.parseInt(this.getOptional("kylin.favorite.statistics-collection-interval.minutes", "60")) * 60;
    }

    public int getFavoriteAccelerateBatchSize() {
        return Integer.valueOf(this.getOptional("kylin.favorite.batch-accelerate-size", "500"));
    }

    // unit of minute
    public long getQueryHistoryScanPeriod() {
        return Long.valueOf(this.getOptional("kylin.favorite.query-history-scan-period.minutes", "60")) * 60 * 1000L;
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
    public String getCoadhaleMetricsReportClassesNames() {
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

    public boolean isShowingGuiTraceToggle() {
        return Boolean.valueOf(getOptional("kylin.htrace.show-gui-trace-toggle", "false"));
    }

    public boolean isHtraceTracingEveryQuery() {
        return Boolean.valueOf(getOptional("kylin.htrace.trace-every-query", "false"));
    }

    public String getHdfsWorkingDirectory(String project) {
        if (isProjectIsolationEnabled() && project != null) {
            return new Path(getHdfsWorkingDirectory(), project).toString() + "/";
        } else {
            return getHdfsWorkingDirectory();
        }
    }

    public boolean isProjectIsolationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.metadata.project-isolation-enable", "true"));
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
        return getOptional("kylin.storage.columnar.jdbc.file-system", "");
    }

    public String getPropertiesWhiteList() {
        return getOptional("kylin.web.properties.whitelist",
                "kylin.web.timezone,kylin.query.cache-enabled,kylin.env,kylin.web.hive-limit,kylin.storage.default,kylin.engine.default,kylin.web.link-hadoop,kylin.web.link-diagnostic,"
                        + "kylin.web.contact-mail,kylin.web.help.length,kylin.web.help.0,kylin.web.help.1,kylin.web.help.2,kylin.web.help.3,"
                        + "kylin.web.help,kylin.web.hide-measures,kylin.web.link-streaming-guide,kylin.server.external-acl-provider,kylin.security.profile,"
                        + "kylin.htrace.show-gui-trace-toggle,kylin.web.export-allow-admin,kylin.web.export-allow-other");
    }

    public boolean getEventAutoApproved() {
        return Boolean.valueOf(getOptional("kylin.event.auto-approved", "true"));
    }

    // newten new add
    public boolean isParquetSeparateFsEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.storage.columnar.separate-fs-enable", "false"));
    }

    public Boolean isCalciteInClauseEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite-in-clause-enabled", "true"));
    }

    public Boolean isCalciteConvertMultipleColumnsIntoOrEnabled() {
        return Boolean
                .parseBoolean(getOptional("kylin.query.calcite-convert-multiple-columns-in-to-or-enabled", "true"));
    }

    public Boolean isEnumerableRulesEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite.enumerable-rules-enabled", "false"));
    }

    public boolean isReduceExpressionsRulesEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.query.calcite.reduce-rules-enabled", "true"));
    }

    public int getEventPollIntervalSecond() {
        return Integer.parseInt(getOptional("kylin.job.event.poll-interval-second", "60"));
    }

    public boolean isCustomizedGcStrategyEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.garbage.customized-strategy-enabled", "false"));
    }

    public double getLayoutSimilarityThreshold() {
        return safeParseDouble(getOptional("kylin.garbage.layout-relative-similarity-threshold"), SIMILARITY_THRESHOLD);
    }

    public long getSimilarityStrategyRejectThreshold() {
        return safeParseLong(getOptional("kylin.garbage.reject-similarity-threshold"), REJECT_SIMILARITY_THRESHOLD);
    }

    public String getLowFreqGarbageStrategyTarget() {
        return getOptional("kylin.garbage.low-freq-strategy-target", "");
    }

    public String getIncludedGarbageStrategyTarget() {
        return getOptional("kylin.garbage.included-strategy-target", "");
    }

    public String getSimilarGarbageStrategyTarget() {
        return getOptional("kylin.garbage.similar-strategy-target", "");
    }

    public boolean isOnlyTailorAggIndex() {
        return Boolean.parseBoolean(getOptional("kylin.garbage.only-tailor-agg-index", "false"));
    }

    public boolean isRemoveIncludedTableIndexEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.garbage.remove-included-table-index", "false"));
    }

    public long getExecutableSurvivalTimeThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.garbage.storage.executable-survival-time-threshold", "30d"),
                TimeUnit.MILLISECONDS);
    }

    public long getStorageQuotaSize() {
        return Long.parseLong(getOptional("kylin.storage.quota-in-giga-bytes", "10240")) * 1024 * 1024 * 1024;
    }

    public long getCuboidLayoutSurvivalTimeThreshold() {
        return TimeUtil.timeStringAs(getOptional("kylin.garbage.storage.cuboid-layout-survival-time-threshold", "7d"),
                TimeUnit.MILLISECONDS);
    }

    public boolean getJobDataLoadEmptyNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-on-empty-data-load", "false"));
    }

    public boolean getJobErrorNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-on-job-error", "false"));
    }

    public boolean getJobSourceRecordsChangeNotificationEnabled() {
        return Boolean.parseBoolean(getOptional("kylin.job.notification-on-source-records-change", "false"));
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
        return Boolean.parseBoolean(getOptional("kylin.metadata.broken-model-deleted-on-smart-mode", "false"));
    }

    public int getPersistFlatTableThreshold() {
        return Integer.parseInt(getOptional("kap.engine.persist-flattable-threshold", "1"));
    }

    public boolean isPersistFlatViewEnabled() {
        return Boolean.parseBoolean(getOptional("kap.engine.persist-flatview", "false"));
    }

    public boolean isShardingJoinOptEnabled() {
        return Boolean.parseBoolean(getOptional("kap.storage.columnar.expose-sharding-trait", "true"));
    }

    public int getQueryPartitionSplitSizeMB() {
        return Integer.parseInt(getOptional("kap.storage.columnar.partition-split-size-mb", "64"));
    }

    public String getStorageProvider() {
        return getOptional("kap.storage.provider", "org.apache.kylin.common.storage.DefaultStorageProvider");
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
        return Long.parseLong(getOptional("kap.table.load-hive-tablename-interval-seconds", "3600"));
    }

    public boolean getLoadHiveTablenameEnabled() {
        return Boolean.parseBoolean(getOptional("kap.table.load-hive-tablename-enabled", "true"));
    }

    private double safeParseDouble(String value, double defaultValue) {
        double rst = defaultValue;
        try {
            rst = Double.parseDouble(value.trim());
        } catch (Exception e) {
            logger.error("Detect a malformed double value, set to a default value {}", defaultValue);
        }
        return rst;
    }

    private long safeParseLong(String value, long defaultValue) {
        long rst = defaultValue;
        try {
            rst = Long.parseLong(value.trim());
        } catch (Exception e) {
            logger.error("Detect a malformed long value, set to a default value {}", defaultValue);
        }
        return rst;
    }
}
