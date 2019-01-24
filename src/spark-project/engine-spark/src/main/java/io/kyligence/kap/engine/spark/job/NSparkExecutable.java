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

package io.kyligence.kap.engine.spark.job;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import com.google.common.collect.Maps;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinConfigExt;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.StringUtil;
import org.apache.kylin.job.common.PatternedLogger;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.AbstractExecutable;
import org.apache.kylin.job.execution.ExecutableContext;
import org.apache.kylin.job.execution.ExecuteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

/**
 */
public class NSparkExecutable extends AbstractExecutable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkExecutable.class);

    protected void setProjectParam() {
        this.setParam(NBatchConstants.P_PROJECT_NAME, getProject());
    }

    protected void setSparkSubmitClassName(String className) {
        this.setParam(NBatchConstants.P_CLASS_NAME, className);
    }

    public String getSparkSubmitClassName() {
        return this.getParam(NBatchConstants.P_CLASS_NAME);
    }

    protected void setJars(String jars) {
        this.setParam(NBatchConstants.P_JARS, jars);
    }

    public String getJars() {
        return this.getParam(NBatchConstants.P_JARS);
    }

    protected void setDistMetaUrl(String metaUrl) {
        this.setParam(NBatchConstants.P_DIST_META_URL, metaUrl);
        this.setParam(NBatchConstants.P_OUTPUT_META_URL, metaUrl + "_output");
    }

    public String getDistMetaUrl() {
        return this.getParam(NBatchConstants.P_DIST_META_URL);
    }

    @Override
    protected ExecuteResult doWork(ExecutableContext context) throws ExecuteException {
        final KylinConfig config = wrapConfig(context);

        String sparkHome = KylinConfig.getSparkHome();
        if (StringUtils.isEmpty(sparkHome) && !config.isUTEnv()) {
            throw new RuntimeException("Missing spark home");
        }

        String kylinJobJar = config.getKylinJobJarPath();
        if (StringUtils.isEmpty(kylinJobJar) && !config.isUTEnv()) {
            throw new RuntimeException("Missing kylin job jar");
        }

        String hadoopConf = System.getProperty("kylin.hadoop.conf.dir");
        if (StringUtils.isEmpty(hadoopConf) && !config.isUTEnv()) {
            throw new RuntimeException(
                    "kylin_hadoop_conf_dir is empty, check if there's error in the output of 'kylin.sh start'");
        }

        File hiveConfFile = new File(hadoopConf, "hive-site.xml");
        if (!hiveConfFile.exists() && !config.isUTEnv()) {
            throw new RuntimeException("Cannot find hive-site.xml in kylin_hadoop_conf_dir: " + hadoopConf + //
                    ". In order to enable spark cubing, you must set kylin.env.hadoop-conf-dir to a dir which contains at least core-site.xml, hdfs-site.xml, hive-site.xml, mapred-site.xml, yarn-site.xml");
        }

        String jars = getJars();
        if (StringUtils.isEmpty(jars)) {
            jars = kylinJobJar;
        }

        try {
            attachMetadataAndKylinProps(config);
        } catch (IOException e) {
            throw new ExecuteException("meta dump failed", e);
        }
        dumpCuboidLayoutIdsIfNeed();
        if (config.isUTEnv()) {
            return runLocalMode(formatAppArgsForSparkLocal());
        } else {
            String[] appArgs = formatAppArgs();
            return runSparkSubmit(config, sparkHome, hadoopConf, jars, kylinJobJar, appArgs, getParent().getId());
        }
    }

    void dumpCuboidLayoutIdsIfNeed() throws ExecuteException {
        if (getParams().containsKey(NBatchConstants.P_LAYOUT_IDS)) {
            File tmpDir = null;
            try {
                tmpDir = File.createTempFile(NBatchConstants.P_LAYOUT_IDS, "");
                FileUtils.writeByteArrayToFile(tmpDir,
                        getParam(NBatchConstants.P_LAYOUT_IDS).getBytes(StandardCharsets.UTF_8));
                int layoutSize = NSparkCubingUtil.str2Longs(getParam(NBatchConstants.P_LAYOUT_IDS)).size();
                getParams().remove(NBatchConstants.P_LAYOUT_IDS);
                setParam(NBatchConstants.P_LAYOUT_ID_PATH, tmpDir.getCanonicalPath());
                logger.info("Layout size :" + layoutSize);
            } catch (IOException e) {
                if (tmpDir != null && tmpDir.exists()) {
                    tmpDir.delete();
                }
                throw new ExecuteException("Write cuboidLayoutIds failed", e);
            }

        }
    }

    protected KylinConfig wrapConfig(ExecutableContext context) {
        val originalConfig = context.getConfig();
        KylinConfigExt kylinConfigExt = null;
        val project = getProject();
        Preconditions.checkState(StringUtils.isNotBlank(project), "job " + getId() + " project info is empty");
        val dataflow = getParam(NBatchConstants.P_DATAFLOW_ID);
        if (StringUtils.isNotBlank(dataflow)) {
            val dataflowManager = NDataflowManager.getInstance(originalConfig, project);
            kylinConfigExt = dataflowManager.getDataflow(dataflow).getConfig();
        } else {
            val projectInstance = NProjectManager.getInstance(originalConfig).getProject(project);
            kylinConfigExt = projectInstance.getConfig();
        }

        val jobOverrides = Maps.<String, String> newHashMap();
        val parentId = getParentId();
        jobOverrides.put("job.id", StringUtils.defaultIfBlank(parentId, getId()));
        if (StringUtils.isNotBlank(parentId)) {
            jobOverrides.put("job.stepId", getId());
        }
        jobOverrides.putAll(kylinConfigExt.getExtendedOverrides());
        return KylinConfigExt.createInstance(kylinConfigExt, jobOverrides);
    }

    private ExecuteResult runSparkSubmit(KylinConfig config, String sparkHome, String hadoopConf, String jars,
            String kylinJobJar, String[] appArgs, String jobId) {

        PatternedLogger patternedLogger = new PatternedLogger(logger);
        try {
            String cmd = generateSparkCmd(config, hadoopConf, jars, kylinJobJar, appArgs);

            CliCommandExecutor exec = new CliCommandExecutor();
            Pair<Integer, String> result = exec.execute(cmd, patternedLogger, jobId);

            Preconditions.checkState(result.getFirst() == 0);
            Map<String, String> extraInfo = makeExtraInfo(patternedLogger.getInfo());
            val ret = ExecuteResult.createSucceed(result.getSecond());
            ret.getExtraInfo().putAll(extraInfo);
            return ret;
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    private String generateSparkCmd(KylinConfig config, String hadoopConf, String jars, String kylinJobJar,
            String[] appArgs) {
        StringBuilder sb = new StringBuilder();
        sb.append("export HADOOP_CONF_DIR=%s && %s/bin/spark-submit --class org.apache.kylin.common.util.SparkEntry ");

        Map<String, String> sparkConfs = config.getSparkConfigOverride();
        for (Map.Entry<String, String> entry : sparkConfs.entrySet()) {
            appendSparkConf(sb, entry.getKey(), entry.getValue());
        }
        appendSparkConf(sb, "spark.executor.extraClassPath", Paths.get(kylinJobJar).getFileName().toString());

        sb.append("--jars %s %s %s");
        String cmd = String.format(sb.toString(), hadoopConf, KylinConfig.getSparkHome(), jars, kylinJobJar,
                StringUtil.join(Arrays.asList(appArgs), " "));
        logger.debug("spark submit cmd: {}", cmd);
        return cmd;
    }

    private void appendSparkConf(StringBuilder sb, String key, String value) {
        // Multiple parameters in "--conf" need to be enclosed in single quotes
        sb.append(" --conf '").append(key).append("=").append(value).append("' ");
    }

    private ExecuteResult runLocalMode(String[] appArgs) {
        try {
            Class<? extends Object> appClz = ClassUtil.forName(getSparkSubmitClassName(), Object.class);
            appClz.getMethod("main", String[].class).invoke(null, (Object) appArgs);
            return ExecuteResult.createSucceed();
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    private String[] formatAppArgsForSparkLocal() {
        List<String> appArgs = new ArrayList<>();
        for (Map.Entry<String, String> entry : getParams().entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();

            if (k.equals(NBatchConstants.P_JARS)) {
                continue; // JARS is for spark-submit, not for app
            }

            if (k.equals(NBatchConstants.P_CLASS_NAME)) {
                continue;
            }

            if (k.equals(AbstractExecutable.PARENT_ID)) {
                continue;
            }

            appArgs.add("-" + k);
            appArgs.add(v);
        }
        return (String[]) appArgs.toArray(new String[appArgs.size()]);
    }

    private String[] formatAppArgs() {
        List<String> appArgs = new ArrayList<>();
        for (Map.Entry<String, String> entry : getParams().entrySet()) {
            String k = entry.getKey();
            String v = entry.getValue();
            switch (k) {
            case NBatchConstants.P_CLASS_NAME:
                appArgs.add(0, v);
                appArgs.add(0, "-" + k);
            case NBatchConstants.P_JARS:
                // JARS is for spark-submit, not for app
                continue;
            case PARENT_ID:
                // JARS is for spark-submit, not for app
                continue;
            default:
                appArgs.add("-" + k);
                appArgs.add(v);
            }
        }
        return (String[]) appArgs.toArray(new String[appArgs.size()]);
    }

    protected Set<String> getMetadataDumpList(KylinConfig config) {
        return Collections.emptySet();
    }

    private void attachMetadataAndKylinProps(KylinConfig config) throws IOException {
        Set<String> dumpList = getMetadataDumpList(config);
        if (dumpList.isEmpty()) {
            return;
        }

        String metaDumpUrl = getDistMetaUrl();
        if (StringUtils.isEmpty(metaDumpUrl)) {
            throw new RuntimeException("Missing metaUrl");
        }

        File tmpDir = File.createTempFile("kylin_job_meta", "");
        FileUtils.forceDelete(tmpDir); // we need a directory, so delete the file first

        Properties props = config.exportToProperties();
        props.setProperty("kylin.metadata.url", metaDumpUrl);
        // dump metadata
        ResourceStore.dumpResources(config, tmpDir, dumpList, props);

        // copy metadata to target metaUrl
        KylinConfig dstConfig = KylinConfig.createKylinConfig(props);
        ResourceStore.createMetadataStore(dstConfig, MetadataStore.ALL_NAMESPACE).uploadFromFile(tmpDir);
        // clean up
        logger.debug("Copied metadata to the target metaUrl, delete the temp dir: {}", tmpDir);
        FileUtils.forceDelete(tmpDir);
    }
}
