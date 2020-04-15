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
package io.kyligence.kap.rest.source;

import static io.kyligence.kap.rest.security.KerberosLoginManager.KEYTAB_SUFFIX;

import java.io.File;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exceptions.KylinException;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Maps;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.response.NHiveTableNameResponse;
import io.kyligence.kap.rest.security.KerberosLoginManager;
import lombok.val;

public class NHiveTableName implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(NHiveTableName.class);
    private static final NHiveTableName instance = new NHiveTableName();
    private Map<String, NHiveSourceInfo> sourceInfos;//Map<UGI,NHiveSourceInfo>
    private volatile Map<String, Boolean> isRunnings;
    private volatile Map<String, Long> lastLoadTimes;
    private final ISourceMetadataExplorer explr;

    public static NHiveTableName getInstance() {
        return instance;
    }

    private NHiveTableName() {
        this(SourceFactory.getSparkSource().getSourceMetadataExplorer());
    }

    private NHiveTableName(ISourceMetadataExplorer explr) {
        this.sourceInfos = new ConcurrentHashMap<>();
        this.isRunnings = new ConcurrentHashMap<>();
        this.lastLoadTimes = new ConcurrentHashMap<>();
        this.explr = explr;
    }

    public void loadHiveTableName() throws Exception {
        logger.warn("Call loadHiveTableName");
        checkIsAllNode();
        logger.info("Now start load hive table name");
        long now = System.currentTimeMillis();
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        Map<String, UserGroupInformation> ugiInfos = Maps.newHashMap();
        ugiInfos.put(UserGroupInformation.getLoginUser().getUserName(), UserGroupInformation.getLoginUser());
        projectManager.listAllProjects().stream().filter(p -> StringUtils.isNotBlank(p.getPrincipal()))
                .forEach(project -> {
                    try {
                        UserGroupInformation ugi = KerberosLoginManager.getInstance().getProjectUGI(project.getName());
                        ugiInfos.put(ugi.getUserName(), ugi);
                    } catch (Exception e) {
                        logger.error("The kerberos information of the project {} is incorrect.", project.getName());
                    }
                });

        ugiInfos.entrySet().forEach(info -> {
            NHiveSourceInfo hiveSourceInfo = fetchUGITables(info.getValue());
            setAllTables(hiveSourceInfo, info.getKey());
        });

        long timeInterval = (System.currentTimeMillis() - now) / 1000;
        logger.info(String.format("Load hive table name successful within %d second", timeInterval));
    }

    private void checkIsAllNode() {
        boolean isAllNode = KylinConfig.getInstanceFromEnv().getServerMode().equals(Constant.SERVER_MODE_ALL);
        if (!isAllNode) {
            throw new RuntimeException("Only all node can load hive table name");
        }
        if (!KylinConfig.getInstanceFromEnv().getLoadHiveTablenameEnabled()) {
            throw new KylinException("KE-1005", MsgPicker.getMsg().getINVALID_LOAD_HIVE_TABLE_NAME());
        }
    }

    private void checkKerberosInfo(String project) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        NProjectManager projectManager = NProjectManager.getInstance(config);
        ProjectInstance projectInstance = projectManager.getProject(project);
        String principal = projectInstance.getPrincipal();
        String keytab = projectInstance.getKeytab();
        try {
            if (config.getKerberosProjectLevelEnable() && StringUtils.isNotBlank(principal)
                    && StringUtils.isNotBlank(keytab)) {
                String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();
                val keytabPath = new Path(kylinConfHome, principal + KEYTAB_SUFFIX).toString();
                File kFile = new File(keytabPath);
                if (!kFile.exists()) {
                    FileUtils.writeStringToFile(kFile, keytab);
                }
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keytabPath);
            }
        } catch (Exception e) {
            throw new KylinException("KE-1042", "The project " + project + " kerberos information has expired.");
        }
    }

    public NHiveTableNameResponse fetchTablesImmediately(UserGroupInformation ugi, String project, boolean force) {
        logger.info("Load hive tables immediately {}, force: {}", project, force);
        checkIsAllNode();
        checkKerberosInfo(project);
        NHiveTableNameResponse response = new NHiveTableNameResponse();
        String ugiName = ugi.getUserName();
        if (isRunnings.get(ugiName) == null || lastLoadTimes.get(ugiName) == null) {
            isRunnings.put(ugiName, false);
            lastLoadTimes.put(ugiName, 0L);
        }

        if (force && !isRunnings.get(ugiName)) {
            isRunnings.put(ugiName, true);
            NHiveSourceInfo sourceInfo = fetchUGITables(ugi);
            setAllTables(sourceInfo, ugiName);
            isRunnings.put(ugiName, false);
            lastLoadTimes.put(ugiName, System.currentTimeMillis());
        }

        response.setIsRunning(isRunnings.get(ugiName));
        response.setTime(System.currentTimeMillis() - lastLoadTimes.get(ugiName));

        return response;
    }

    public NHiveSourceInfo fetchUGITables(UserGroupInformation ugi) {
        logger.info("Load hive tables from ugi {}", ugi.getUserName());
        NHiveSourceInfo sourceInfo = null;
        if (UserGroupInformation.isSecurityEnabled()) {
            sourceInfo = ugi.doAs(new PrivilegedAction<NHiveSourceInfo>() {
                @Override
                public NHiveSourceInfo run() {
                    return fetchTables();
                }
            });

        } else {
            sourceInfo = fetchTables();
        }

        return sourceInfo;
    }

    public boolean checkExistsTablesAccess(UserGroupInformation ugi, String project) {
        val kapConfig = KapConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(kapConfig.getKylinConfig());
        return ugi.doAs(new PrivilegedAction<Boolean>() {
            @Override
            public Boolean run() {
                val tables = projectManager.getProject(project).getTables();
                return explr.checkTablesAccess(tables);
            }
        });
    }

    public NHiveSourceInfo fetchTables() {
        NHiveSourceInfo sourceInfo = new NHiveSourceInfo();
        try {
            List<String> dbs = explr.listDatabases().stream().map(String::toUpperCase).collect(Collectors.toList());
            Map<String, List<String>> newTables = listTables(dbs);
            sourceInfo.setTables(newTables);
        } catch (Exception e) {
            logger.error("Load hive tables error.", e);
        }

        return sourceInfo;
    }

    private Map<String, List<String>> listTables(List<String> dbs) throws Exception {
        Map<String, List<String>> newTables = new HashMap<>();
        for (String db : dbs) {
            if (explr.checkDatabaseAccess(db)) {
                List<String> tbs = explr.listTables(db).stream().map(String::toUpperCase).collect(Collectors.toList());
                if (!tbs.isEmpty()) {
                    newTables.put(db, tbs);
                }
                int currDBSize = newTables.keySet().size();
                if (currDBSize % 20 == 0) {
                    logger.info("Foreach database curr pos {}, total num {}", currDBSize, dbs.size());
                }
            }
        }

        return newTables;
    }

    public synchronized List<String> getTables(UserGroupInformation ugi, String currName, String db) {
        List<String> result = null;
        if (sourceInfos.get(ugi.getUserName()) != null) {
            result = sourceInfos.get(ugi.getUserName()).getDatabaseInfo(db);
        }

        return result != null ? result : new ArrayList<>();
    }

    public synchronized void setAllTables(NHiveSourceInfo sourceInfo, String currName) {
        sourceInfos.put(currName, sourceInfo);
    }

    @VisibleForTesting
    public synchronized void setAllTablesForTest(NHiveSourceInfo sourceInfo, String currName) {
        sourceInfos.put(currName, sourceInfo);
    }

    @Override
    public void run() {
        try {
            loadHiveTableName();
        } catch (Exception e) {
            logger.error("msg", e);
        }
    }
}
