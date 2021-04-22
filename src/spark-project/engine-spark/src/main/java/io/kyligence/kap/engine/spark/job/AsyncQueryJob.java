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

import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.directory.api.util.Strings;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.BufferedLogger;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.job.exception.ExecuteException;
import org.apache.kylin.job.execution.ExecuteResult;
import org.apache.kylin.job.execution.JobTypeEnum;
import org.apache.kylin.query.util.QueryParams;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.collect.Lists;

import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.metadata.cube.model.NBatchConstants;
import lombok.val;

public class AsyncQueryJob extends NSparkExecutable {

    private static final Logger logger = LoggerFactory.getLogger(AsyncQueryJob.class);
    private static final String GLOBAL = "/_global";
    private static final String DATAFLOW = "/dataflow";
    private static final String DATAFLOW_DETAIL = "/dataflow_details";
    private static final String INDEX_PLAN = "/index_plan";
    private static final String MODEL = "/model_desc";
    private static final String TABLE = "/table";
    private static final String TABLE_EXD = "/table_exd";
    private static final String[] META_DUMP_LIST = new String[] { DATAFLOW, DATAFLOW_DETAIL, INDEX_PLAN, MODEL, TABLE,
            TABLE_EXD };

    @Override
    protected ExecuteResult runSparkSubmit(String hadoopConf, String jars, String kylinJobJar, String appArgs) {
        val patternedLogger = new BufferedLogger(logger);

        try {
            killOrphanApplicationIfExists(getId());
            String cmd = generateSparkCmd(hadoopConf, jars, kylinJobJar, appArgs);
            CliCommandExecutor exec = new CliCommandExecutor();
            CliCommandExecutor.CliCmdExecResult r = exec.execute(cmd, patternedLogger, getId());
            return ExecuteResult.createSucceed(r.getCmd());
        } catch (Exception e) {
            return ExecuteResult.createError(e);
        }
    }

    @Override
    protected Map<String, String> getSparkConfigOverride(KylinConfig config) {
        Map<String, String> overrides = config.getAsyncQuerySparkConfigOverride();
        if (!overrides.containsKey("spark.driver.memory")) {
            overrides.put("spark.driver.memory", "1024m");
        }

        if (UserGroupInformation.isSecurityEnabled()) {
            overrides.put("spark.hadoop.hive.metastore.sasl.enabled", "true");
        }
        return overrides;
    }

    @Override
    protected String getJobNamePrefix() {
        return "ASYNC_QUERY_JOB_";
    }

    public ExecuteResult submit(QueryParams queryParams) throws ExecuteException, JsonProcessingException {
        this.setLogPath(getSparkDriverLogHdfsPath(getConfig()));
        KylinConfig config = getConfig();
        String kylinJobJar = config.getKylinJobJarPath();
        if (StringUtils.isEmpty(kylinJobJar) && !config.isUTEnv()) {
            throw new RuntimeException("Missing kylin job jar");
        }
        String jars = getJars();
        if (StringUtils.isEmpty(jars)) {
            jars = kylinJobJar;
        }

        ObjectMapper fieldOnlyMapper = new ObjectMapper();
        fieldOnlyMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
        setParam(NBatchConstants.P_QUERY_PARAMS, fieldOnlyMapper.writeValueAsString(queryParams));
        setParam(NBatchConstants.P_QUERY_CONTEXT, JsonUtil.writeValueAsString(QueryContext.current()));
        setParam(NBatchConstants.P_PROJECT_NAME, getProject());
        setParam(NBatchConstants.P_QUERY_ID, QueryContext.current().getQueryId());
        setParam(NBatchConstants.P_JOB_ID, getId());
        setParam(NBatchConstants.P_JOB_TYPE, JobTypeEnum.ASYNC_QUERY.toString());
        setDistMetaUrl(config.getJobTmpMetaStoreUrl(getProject(), getId()));

        try {
            // dump kylin.properties to HDFS
            attachMetadataAndKylinProps(config, true);

            // dump metadata to HDFS
            List<String> metadataDumpSet = Lists.newArrayList();
            ResourceStore resourceStore = ResourceStore.getKylinMetaStore(config);
            metadataDumpSet.addAll(resourceStore.listResourcesRecursively(GLOBAL));
            for (String mata : META_DUMP_LIST) {
                if (resourceStore.listResourcesRecursively("/" + getProject() + mata) != null) {
                    metadataDumpSet.addAll(resourceStore.listResourcesRecursively("/" + getProject() + mata));
                }
            }
            config.setMetadataUrl(config.getJobTmpMetaStoreUrl(getProject(), getId()).toString());
            MetadataStore.createMetadataStore(config).dump(ResourceStore.getKylinMetaStore(config), metadataDumpSet);
        } catch (Exception e) {
            throw new ExecuteException("kylin properties or meta dump failed", e);
        }

        return runSparkSubmit(getHadoopConfDir(), jars, kylinJobJar,
                "-className io.kyligence.kap.engine.spark.job.AsyncQueryApplication "
                        + createArgsFileOnHDFS(config, getId()));
    }

    private String getHadoopConfDir() {
        KylinConfig kylinconfig = KylinConfig.getInstanceFromEnv();
        if (Strings.isNotEmpty(kylinconfig.getAsyncQueryHadoopConfDir())) {
            return kylinconfig.getAsyncQueryHadoopConfDir();
        }
        return HadoopUtil.getHadoopConfDir();
    }
}
