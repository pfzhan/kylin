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
import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CHECK_KERBEROS;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;

import java.io.File;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
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
    private Map<String, NHiveSourceInfo> sourceInfos;//Map<UGI/projectName,NHiveSourceInfo>
    private volatile Map<String, Boolean> isRunnings;
    private volatile Map<String, Long> lastLoadTimes;
    private ISourceMetadataExplorer explr;
    //only jdbc source use project name as key
    private static final Set<Integer> USE_PROJECT_AS_KEY_SOURCE_TYPE = Stream.of(8).collect(Collectors.toSet());

    public static NHiveTableName getInstance() {
        return Singletons.getInstance(NHiveTableName.class);
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
        Map<String, Pair<String, UserGroupInformation>> ugiInfos = Maps.newHashMap();
        ugiInfos.put(UserGroupInformation.getLoginUser().getUserName(),
                Pair.newPair("", UserGroupInformation.getLoginUser()));
        projectManager.listAllProjects().stream().filter(p -> StringUtils.isNotBlank(p.getPrincipal()))
                .forEach(project -> {
                    try {
                        UserGroupInformation ugi = KerberosLoginManager.getInstance().getProjectUGI(project.getName());
                        ugiInfos.put(geHiveInfoKeyByProjectName(project.getName()),
                                Pair.newPair(project.getName(), ugi));
                    } catch (Exception e) {
                        logger.error("The kerberos information of the project {} is incorrect.", project.getName());
                    }
                });

        ugiInfos.entrySet().forEach(info -> {
            val pair = info.getValue();
            NHiveSourceInfo hiveSourceInfo = fetchUGITables(pair.getSecond(), getHiveFilterList(pair.getFirst()));
            setAllTables(hiveSourceInfo, info.getKey());
        });

        long timeInterval = (System.currentTimeMillis() - now) / 1000;
        logger.info("Load hive table name successful within {} second", timeInterval);
    }

    private List<String> getHiveFilterList(String project) {
        if (StringUtils.isEmpty(project)) {
            return Collections.emptyList();
        }
        val config = KylinConfig.getInstanceFromEnv();
        String[] databases = NProjectManager.getInstance(config).getProject(project).getConfig().getHiveDatabases();
        if (databases.length == 0) {
            databases = config.getHiveDatabases();
        }
        return Arrays.stream(databases) //
                .map(str -> str.toUpperCase(Locale.ROOT)) //
                .collect(Collectors.toList());
    }

    private void checkIsAllNode() {
        if (!KylinConfig.getInstanceFromEnv().isJobNode()) {
            throw new RuntimeException("Only job/all node can load hive table name");
        }
        if (!KylinConfig.getInstanceFromEnv().getLoadHiveTablenameEnabled()) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getINVALID_LOAD_HIVE_TABLE_NAME());
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
            throw new KylinException(FAILED_CHECK_KERBEROS,
                    "The project " + project + " kerberos information has expired.");
        }
    }

    public NHiveTableNameResponse fetchTablesImmediately(UserGroupInformation ugi, String project, boolean force) {
        logger.info("Load hive tables immediately {}, force: {}", project, force);
        checkIsAllNode();
        checkKerberosInfo(project);
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        this.explr = SourceFactory.getSource(projectManager.getProject(project)).getSourceMetadataExplorer();
        logger.info("Use source explorer {}", explr.getClass().getCanonicalName());
        NHiveTableNameResponse response = new NHiveTableNameResponse();
        String keyName = geHiveInfoKeyByProjectName(project);
        if (isRunnings.get(keyName) == null || lastLoadTimes.get(keyName) == null) {
            isRunnings.put(keyName, false);
            lastLoadTimes.put(keyName, 0L);
        }

        if (force && !isRunnings.get(keyName)) {
            isRunnings.put(keyName, true);
            NHiveSourceInfo sourceInfo = fetchUGITables(ugi, getHiveFilterList(project));
            setAllTables(sourceInfo, keyName);
            isRunnings.put(keyName, false);
            lastLoadTimes.put(keyName, System.currentTimeMillis());
        }

        response.setIsRunning(isRunnings.get(keyName));
        response.setTime(System.currentTimeMillis() - lastLoadTimes.get(keyName));

        return response;
    }

    public NHiveSourceInfo fetchUGITables(UserGroupInformation ugi, List<String> filterList) {
        logger.info("Load hive tables from ugi {}", ugi.getUserName());
        NHiveSourceInfo sourceInfo = null;
        if (UserGroupInformation.isSecurityEnabled()) {
            sourceInfo = ugi.doAs((PrivilegedAction<NHiveSourceInfo>) () -> fetchTables(filterList));
        } else {
            sourceInfo = fetchTables(filterList);
        }

        return sourceInfo;
    }

    public boolean checkExistsTablesAccess(UserGroupInformation ugi, String project) {
        val kapConfig = KapConfig.getInstanceFromEnv();
        val projectManager = NProjectManager.getInstance(kapConfig.getKylinConfig());
        return ugi.doAs((PrivilegedAction<Boolean>) () -> {
            val tables = projectManager.getProject(project).getTables();
            return explr.checkTablesAccess(tables);
        });
    }

    public NHiveSourceInfo fetchTables(List<String> filterList) {
        NHiveSourceInfo sourceInfo = new NHiveSourceInfo();
        try {
            List<String> dbs = explr.listDatabases().stream().map(str -> str.toUpperCase(Locale.ROOT))
                    .filter(str -> filterList.isEmpty() || filterList.contains(str)) //
                    .collect(Collectors.toList());
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
                List<String> tbs = explr.listTables(db).stream().map(str -> str.toUpperCase(Locale.ROOT))
                        .collect(Collectors.toList());
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

    public synchronized List<String> getTables(String project, String db) {
        String keyName = geHiveInfoKeyByProjectName(project);
        List<String> result = null;
        if (sourceInfos.get(keyName) != null) {
            result = sourceInfos.get(keyName).getDatabaseInfo(db);
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

    private String geHiveInfoKeyByProjectName(String project) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        Integer souceType = projectManager.getProject(project).getSourceType();
        if (USE_PROJECT_AS_KEY_SOURCE_TYPE.contains(souceType)) {
            return "project#" + project;
        } else {
            return "ugi#" + KerberosLoginManager.getInstance().getProjectUGI(project).getUserName();
        }
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
