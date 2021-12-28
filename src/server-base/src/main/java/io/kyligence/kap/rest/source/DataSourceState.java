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

import static org.apache.kylin.common.exception.ServerErrorCode.FAILED_CHECK_KERBEROS;
import static org.apache.kylin.common.exception.ServerErrorCode.PERMISSION_DENIED;
import static org.apache.kylin.common.exception.SystemErrorCode.JOBNODE_API_INVALID;

import java.io.File;
import java.io.IOException;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.time.StopWatch;
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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.rest.response.NHiveTableNameResponse;
import io.kyligence.kap.rest.security.KerberosLoginManager;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataSourceState implements Runnable {

    private static final String JDBC_SOURCE_KEY_PREFIX = "project#";
    private static final String SOURCE_KEY_PREFIX = "ugi#";

    private final StopWatch sw;
    private final Map<String, NHiveSourceInfo> cache;
    private final Map<String, Boolean> runningStateMap;
    private final Map<String, Long> lastLoadTimeMap;
    private ISourceMetadataExplorer explore;

    //only jdbc source use project name as key
    private static final Set<Integer> USE_PROJECT_AS_KEY_SOURCE_TYPE = Sets.newHashSet(8);

    public static DataSourceState getInstance() {
        return Singletons.getInstance(DataSourceState.class);
    }

    private DataSourceState() {
        setExplore(SourceFactory.getSparkSource().getSourceMetadataExplorer());
        this.cache = Maps.newConcurrentMap();
        this.runningStateMap = Maps.newConcurrentMap();
        this.lastLoadTimeMap = Maps.newConcurrentMap();
        this.sw = StopWatch.create();
    }

    @Override
    public void run() {
        try {
            loadAllSourceInfoToCache();
        } catch (Exception e) {
            log.error("Scheduling refresh of hive table name cache failed", e);
        } finally {
            sw.reset();
        }
    }

    /**
     * load all source info to cache
     */
    public void loadAllSourceInfoToCache() throws IOException {
        sw.start();
        checkIsAllNode();
        log.info("start load all table name to cache");

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        KerberosLoginManager kerberosManager = KerberosLoginManager.getInstance();

        Map<String, Pair<ProjectInstance, UserGroupInformation>> ugiMap = Maps.newHashMap();
        ugiMap.put(SOURCE_KEY_PREFIX + UserGroupInformation.getLoginUser().getUserName(),
                Pair.newPair(null, UserGroupInformation.getLoginUser()));

        projectManager.listAllProjects().stream().filter(p -> StringUtils.isNotBlank(p.getPrincipal()))
                .forEach(projectInstance -> {
                    try {
                        UserGroupInformation projectUgi = kerberosManager.getProjectUGI(projectInstance.getName());
                        ugiMap.put(getCacheKeyByProject(projectInstance), Pair.newPair(projectInstance, projectUgi));
                    } catch (Exception e) {
                        log.error("The kerberos information of the project {} is incorrect.",
                                projectInstance.getName());
                    }
                });

        ugiMap.forEach((cacheKey, pair) -> {
            ProjectInstance projectInstance = pair.getFirst();
            UserGroupInformation projectUgi = pair.getSecond();
            runningStateMap.put(cacheKey, true);
            NHiveSourceInfo sourceInfo = fetchUgiSourceInfo(projectUgi, getHiveFilterList(projectInstance));
            putCache(cacheKey, sourceInfo);
            runningStateMap.put(cacheKey, false);
            lastLoadTimeMap.put(cacheKey, System.currentTimeMillis());
        });

        sw.stop();
        log.info("Load hive table name successful within {} second", sw.getTime(TimeUnit.SECONDS));
    }

    public NHiveTableNameResponse loadAllSourceInfoToCacheForced(String project, boolean force) {
        log.info("Load hive tables immediately {}, force: {}", project, force);
        checkIsAllNode();
        checkKerberosInfo(project);

        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);
        NHiveTableNameResponse response = new NHiveTableNameResponse();
        String cacheKey = getCacheKeyByProject(projectInstance);
        runningStateMap.putIfAbsent(cacheKey, false);
        lastLoadTimeMap.putIfAbsent(cacheKey, 0L);

        if (!force) {
            response.setIsRunning(runningStateMap.get(cacheKey));
            response.setTime(System.currentTimeMillis() - lastLoadTimeMap.get(cacheKey));
            return response;
        }
        setExplore(SourceFactory.getSource(projectInstance).getSourceMetadataExplorer());

        KerberosLoginManager kerberosManager = KerberosLoginManager.getInstance();
        UserGroupInformation projectUGI = kerberosManager.getProjectUGI(project);
        runningStateMap.put(cacheKey, true);
        NHiveSourceInfo sourceInfo = fetchUgiSourceInfo(projectUGI, getHiveFilterList(projectInstance));
        putCache(cacheKey, sourceInfo);
        response.setIsRunning(runningStateMap.put(cacheKey, false));
        response.setTime(0L);
        return response;
    }

    public synchronized List<String> getTables(String project, String db) {
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        ProjectInstance projectInstance = projectManager.getProject(project);

        List<String> result = Lists.newArrayList();
        NHiveSourceInfo sourceInfo = cache.get(getCacheKeyByProject(projectInstance));
        if (Objects.nonNull(sourceInfo) && Objects.nonNull(sourceInfo.getDatabaseInfo(db))) {
            result.addAll(sourceInfo.getDatabaseInfo(db));
        }
        return result;
    }

    public synchronized void putCache(String cacheKey, NHiveSourceInfo sourceInfo) {
        cache.put(cacheKey, sourceInfo);
    }

    private String getCacheKeyByProject(ProjectInstance projectInstance) {
        String projectName = projectInstance.getName();
        if (USE_PROJECT_AS_KEY_SOURCE_TYPE.contains(projectInstance.getSourceType())) {
            return JDBC_SOURCE_KEY_PREFIX + projectName;
        } else {
            return SOURCE_KEY_PREFIX + KerberosLoginManager.getInstance().getProjectUGI(projectName).getUserName();
        }
    }

    public NHiveSourceInfo fetchUgiSourceInfo(UserGroupInformation ugi, List<String> filterList) {
        log.info("Load hive tables from ugi {}", ugi.getUserName());
        NHiveSourceInfo sourceInfo;
        if (UserGroupInformation.isSecurityEnabled()) {
            sourceInfo = ugi.doAs((PrivilegedAction<NHiveSourceInfo>) () -> fetchSourceInfo(filterList));
        } else {
            sourceInfo = fetchSourceInfo(filterList);
        }
        return sourceInfo;
    }

    private NHiveSourceInfo fetchSourceInfo(List<String> filterList) {
        NHiveSourceInfo sourceInfo = new NHiveSourceInfo();
        try {
            List<String> databaseList = explore.listDatabases().stream().map(StringUtils::toRootUpperCase)
                    .filter(database -> CollectionUtils.isEmpty(filterList) || filterList.contains(database))
                    .collect(Collectors.toList());
            Map<String, List<String>> dbTableList = listTables(databaseList);
            sourceInfo.setTables(dbTableList);
        } catch (Exception e) {
            log.error("Load hive tables error.", e);
        }
        return sourceInfo;
    }

    private Map<String, List<String>> listTables(List<String> databaseList) throws Exception {
        HashMap<String, List<String>> dbTableList = Maps.newHashMap();
        int databaseTotalSize = databaseList.size();

        for (String database : databaseList) {
            if (!explore.checkDatabaseAccess(database)) {
                continue;
            }
            List<String> tableList = explore.listTables(database).stream().map(StringUtils::toRootUpperCase)
                    .collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(tableList)) {
                dbTableList.put(database, tableList);
            }
            int currDatabaseSize = dbTableList.keySet().size();
            if (currDatabaseSize % 20 == 0) {
                log.info("Foreach database curr pos {}, total num {}", currDatabaseSize, databaseTotalSize);
            }
        }
        return dbTableList;
    }

    private void checkKerberosInfo(String project) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        NProjectManager projectManager = NProjectManager.getInstance(kylinConfig);
        ProjectInstance projectInstance = projectManager.getProject(project);

        String principal = projectInstance.getPrincipal();
        String keytab = projectInstance.getKeytab();
        try {
            if (kylinConfig.getKerberosProjectLevelEnable() && !StringUtils.isAllBlank(principal, keytab)) {
                String kylinConfHome = KapConfig.getKylinConfDirAtBestEffort();

                String keyTabPath = new Path(kylinConfHome, principal.concat(KerberosLoginManager.KEYTAB_SUFFIX))
                        .toString();
                File keyTabFile = new File(keyTabPath);
                if (!keyTabFile.exists()) {
                    FileUtils.writeStringToFile(keyTabFile, keytab);
                }
                UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, keyTabPath);
            }
        } catch (Exception e) {
            throw new KylinException(FAILED_CHECK_KERBEROS,
                    "The project " + project + " kerberos information has expired.");
        }
    }

    /**
     * get filter list from kylin.source.hive.databases
     */
    private List<String> getHiveFilterList(ProjectInstance projectInstance) {
        if (Objects.isNull(projectInstance)) {
            return Collections.emptyList();
        }
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        String[] databases = projectInstance.getConfig().getHiveDatabases();
        if (databases.length == 0) {
            databases = config.getHiveDatabases();
        }
        return Arrays.stream(databases).map(str -> str.toUpperCase(Locale.ROOT)).collect(Collectors.toList());
    }

    private void checkIsAllNode() {
        if (!KylinConfig.getInstanceFromEnv().isJobNode()) {
            throw new KylinException(JOBNODE_API_INVALID, "Only job/all node can load hive table name");
        }
        if (!KylinConfig.getInstanceFromEnv().getLoadHiveTablenameEnabled()) {
            throw new KylinException(PERMISSION_DENIED, MsgPicker.getMsg().getINVALID_LOAD_HIVE_TABLE_NAME());
        }
    }

    private synchronized void setExplore(ISourceMetadataExplorer explore) {
        this.explore = explore;
    }

}
