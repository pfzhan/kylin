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

package io.kyligence.kap.metadata.epoch;

import static org.apache.kylin.common.persistence.ResourceStore.GLOBAL_EPOCH;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.FutureTask;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EpochStartedNotifier;
import io.kyligence.kap.common.scheduler.ProjectControlledNotifier;
import io.kyligence.kap.common.scheduler.ProjectEscapedNotifier;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.val;

public class EpochManager implements IKeep {
    private static final Logger logger = LoggerFactory.getLogger(EpochManager.class);

    private static final Serializer<Epoch> EPOCH_SERIALIZER = new JsonSerializer<Epoch>(Epoch.class);

    public static EpochManager getInstance(KylinConfig config) {
        return config.getManager(EpochManager.class);
    }

    public static final String GLOBAL = "_global";

    private static final ExecutorService pool;

    private Set<String> currentEpochs = Sets.newCopyOnWriteArraySet();

    private boolean started = false;

    // called by reflection
    @SuppressWarnings("unused")
    static EpochManager newInstance(KylinConfig config) {
        return new EpochManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    private String identity;
    private SchedulerEventBusFactory schedulerEventBusFactory;

    static {
        pool = new ThreadPoolExecutor(5, 10, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    }

    private EpochManager(KylinConfig cfg) {
        this.config = cfg;
        this.identity = EpochOrchestrator.getOwnerIdentity();
        schedulerEventBusFactory = SchedulerEventBusFactory.getInstance(config);
    }

    //for test
    public Epoch getGlobalEpoch() {
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).getResource(GLOBAL_EPOCH,
                EPOCH_SERIALIZER);
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class MaintenanceModeResult {
        private boolean successful;
        private String status;
    }

    public Boolean setMaintenanceMode(String reason) {
        return UnitOfWork.doInTransactionWithRetry(() -> {
            Epoch epoch = getGlobalEpoch();
            if (epoch == null || !epoch.getCurrentEpochOwner().equals(identity)) {
                throw new EpochNotMatchException("System is trying to recover, please try again later",
                        EpochManager.GLOBAL);
            }
            if (epoch.isMaintenanceMode()) {
                return Boolean.FALSE;
            }
            epoch.setMaintenanceMode(true);
            epoch.setMaintenanceModeReason(reason);
            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).checkAndPutResource(GLOBAL_EPOCH, epoch,
                    EPOCH_SERIALIZER);
            return Boolean.TRUE;
        }, GLOBAL, 1);
    }

    public Boolean unsetMaintenanceMode(String reason) {
        return UnitOfWork.doInTransactionWithRetry(() -> {
            Epoch epoch = getGlobalEpoch();
            if (epoch == null || !epoch.getCurrentEpochOwner().equals(identity)) {
                throw new EpochNotMatchException("System is trying to recover, please try again later",
                        EpochManager.GLOBAL);
            }
            if (!epoch.isMaintenanceMode()) {
                return Boolean.FALSE;
            }
            epoch.setMaintenanceMode(false);
            epoch.setMaintenanceModeReason(reason);
            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).checkAndPutResource(GLOBAL_EPOCH, epoch,
                    EPOCH_SERIALIZER);
            return Boolean.TRUE;
        }, GLOBAL, 1);
    }

    private boolean updateGlobalEpoch(boolean force) {
        return UnitOfWork.doInTransactionWithRetry(() -> {
            Epoch epoch = getNewEpoch(getGlobalEpoch(), force, GLOBAL);
            if (epoch == null) {
                return false;
            }
            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).checkAndPutResource(GLOBAL_EPOCH, epoch,
                    EPOCH_SERIALIZER);
            return true;
        }, GLOBAL, 1);
    }

    private List<ProjectInstance> getProjectsToMarkOwner() {
        List<ProjectInstance> prjs = NProjectManager.getInstance(config).listAllProjects();
        return prjs.stream()
                .filter(p -> !isEpochLegal(p.getEpoch()) || p.getEpoch().getCurrentEpochOwner().equals(identity))
                .collect(Collectors.toList());
    }

    private void tryUpdatePrjEpochs(Set<String> newEpochs) throws Exception {
        List<ProjectInstance> prjs = getProjectsToMarkOwner();
        Map<String, FutureTask<Boolean>> fts = Maps.newHashMap();
        for (ProjectInstance prj : prjs) {
            FutureTask<Boolean> ft = new FutureTask<Boolean>(new Callable<Boolean>() {
                @Override
                public Boolean call() {
                    return updateProjectEpoch(prj);
                }
            });
            pool.submit(ft);
            fts.put(prj.getName(), ft);
        }
        for (val entry : fts.entrySet()) {
            try {
                if (entry.getValue().get(KylinConfig.getInstanceFromEnv().getUpdateEpochTimeout(), TimeUnit.SECONDS)) {
                    newEpochs.add(entry.getKey());
                }
            } catch (Exception e) {
                logger.error("failed to update {}", entry.getKey(), e);
                if (checkEpochOwner(entry.getKey())) {
                    newEpochs.add(entry.getKey());
                }
            }
        }
    }

    public boolean updateProjectEpoch(ProjectInstance prj) {
        return updateProjectEpoch(prj, false);
    }

    public boolean updateProjectEpoch(ProjectInstance prj, boolean force) {
        try {
            return UnitOfWork.doInTransactionWithRetry(() -> {
                val kylinConfig = KylinConfig.getInstanceFromEnv();
                ProjectInstance project = NProjectManager.getInstance(kylinConfig).getProject(prj.getName());
                if (project == null) {
                    return false;
                }
                Epoch finalEpoch = getNewEpoch(project.getEpoch(), force, prj.getName());
                if (finalEpoch == null) {
                    return false;
                }
                NProjectManager.getInstance(kylinConfig).updateProject(prj.getName(),
                        copyForWrite -> copyForWrite.setEpoch(finalEpoch));
                return true;
            }, prj.getName(), 1);
        } catch (Exception e) {
            logger.error("update " + prj.getName() + " epoch failed.", e);
            return false;
        }
    }

    public boolean tryUpdateGlobalEpoch(Set<String> newEpochs, boolean force) throws Exception {
        val ft = new FutureTask<Boolean>(new Callable<Boolean>() {
            @Override
            public Boolean call() {
                return updateGlobalEpoch(force);
            }
        });
        pool.submit(ft);
        try {
            if (ft.get(KylinConfig.getInstanceFromEnv().getUpdateEpochTimeout(), TimeUnit.SECONDS))
                newEpochs.add(GLOBAL);
            return true;
        } catch (Exception e) {
            logger.error("failed to update epoch {}", GLOBAL, e);
            return false;
        }
    }

    private Epoch getNewEpoch(Epoch epoch, String project) {
        return getNewEpoch(epoch, false, project);
    }

    private Epoch getNewEpoch(Epoch epoch, boolean force, String project) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();

        if (!kylinConfig.getEpochCheckerEnabled()) {
            return new Epoch(1L, identity, Long.MAX_VALUE, kylinConfig.getServerMode(), false, null);
        }

        if (epoch == null) {
            epoch = new Epoch(1L, identity, System.currentTimeMillis(), kylinConfig.getServerMode(), false, null);
        } else {
            if (!epoch.getCurrentEpochOwner().equals(identity)) {
                if (isEpochLegal(epoch) && !force)
                    return null;
                epoch.setEpochId(epoch.getEpochId() + 1);
            } else {
                if (!currentEpochs.contains(project)) {
                    epoch.setEpochId(epoch.getEpochId() + 1);
                }
            }
            epoch.setServerMode(kylinConfig.getServerMode());
            epoch.setLastEpochRenewTime(System.currentTimeMillis());
            epoch.setCurrentEpochOwner(identity);
        }
        return epoch;
    }

    public synchronized void updateAllEpochs() throws Exception {
        logger.info("start updateAllEpochs.........");
        val oriEpochs = Sets.newHashSet(currentEpochs);
        Set<String> newEpochs = Sets.newCopyOnWriteArraySet();
        tryUpdateGlobalEpoch(newEpochs, false);
        tryUpdatePrjEpochs(newEpochs);
        logger.info("Project [" + String.join(",", newEpochs) + "] owned by " + identity);
        Collection<String> retainPrjs = CollectionUtils.retainAll(oriEpochs, newEpochs);
        Collection<String> escapedProjects = CollectionUtils.removeAll(oriEpochs, retainPrjs);
        if (!getGlobalEpoch().isMaintenanceMode()) {
            for (String prj : newEpochs) {
                schedulerEventBusFactory.post(new ProjectControlledNotifier(prj));
            }

            for (String prj : escapedProjects) {
                schedulerEventBusFactory.post(new ProjectEscapedNotifier(prj));
            }
        }

        if (!started) {
            started = true;
            schedulerEventBusFactory.post(new EpochStartedNotifier());
        }
        logger.info("end updateAllEpochs.........");
        currentEpochs = newEpochs;
    }

    public boolean checkEpochOwner(String project) {
        Epoch epoch;
        NProjectManager projectManager = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        checkPrj(project);
        if (project.equals(GLOBAL)) {
            epoch = getGlobalEpoch();
        } else {
            val projectInstance = projectManager.getProject(project);
            if (projectInstance == null) {
                return false;
            }
            epoch = projectInstance.getEpoch();
        }
        return isEpochLegal(epoch) && epoch.getCurrentEpochOwner().equals(identity);
    }

    //when create a project, mark now
    public synchronized void updateEpoch(String project) throws Exception {
        if (StringUtils.isEmpty(project))
            updateAllEpochs();
        if (project.equals(GLOBAL)) {
            if (tryUpdateGlobalEpoch(Sets.newCopyOnWriteArraySet(), false)) {
                currentEpochs.add(project);
                if (!getGlobalEpoch().isMaintenanceMode()) {
                    schedulerEventBusFactory.post(new ProjectControlledNotifier(project));
                }
            }
        }
        ProjectInstance prj = NProjectManager.getInstance(config).getProject(project);
        if (prj == null) {
            throw new IllegalStateException(String.format("Project %s does not exist", project));
        }
        if (updateProjectEpoch(prj)) {
            currentEpochs.add(project);
            if (!getGlobalEpoch().isMaintenanceMode()) {
                schedulerEventBusFactory.post(new ProjectControlledNotifier(project));
            }
        }
    }

    public Set<String> getAllLeadersByMode(String serverMode) {
        NProjectManager prjMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        Set<String> leaders = Sets.newHashSet();
        prjMgr.listAllProjects().stream().map(ProjectInstance::getEpoch).filter(epoch -> isEpochLegal(epoch))
                .map(Epoch::getCurrentEpochOwner).map(s -> getHostAndPort(s)).collect(Collectors.toSet());
        Epoch globalEpoch = getGlobalEpoch();
        if (StringUtils.isNotBlank(serverMode) && StringUtils.isNotBlank(globalEpoch.getServerMode())
                && !serverMode.equals(globalEpoch.getServerMode())) {
            return leaders;
        }
        if (isEpochLegal(globalEpoch)) {
            leaders.add(getHostAndPort(globalEpoch.getCurrentEpochOwner()));
        }
        return leaders;
    }

    public boolean isEpochLegal(Epoch epoch) {
        return epoch != null && StringUtils.isNotEmpty(epoch.getCurrentEpochOwner()) && System.currentTimeMillis()
                - epoch.getLastEpochRenewTime() <= config.getEpochExpireTimeSecond() * 1000;
    }

    public String getEpochOwner(String project) {
        checkPrj(project);
        Epoch epoch;
        if (project.equals(GLOBAL)) {
            epoch = getGlobalEpoch();
        } else {
            NProjectManager projectManager = NProjectManager.getInstance(config);
            ProjectInstance prj = projectManager.getProject(project);
            if (prj == null)
                throw new IllegalArgumentException(String.format("Project %s does not exist", project));
            epoch = prj.getEpoch();
        }
        if (isEpochLegal(epoch)) {
            return getHostAndPort(epoch.getCurrentEpochOwner());
        } else {
            return null;
        }
    }

    public String getHostAndPort(String owner) {
        return owner.split("\\|")[0];
    }

    //ensure only one project thread running
    public boolean checkEpochId(long epochId, String project) {
        return getEpochId(project) == epochId;
    }

    public long getEpochId(String project) {
        checkPrj(project);
        Epoch epoch = null;
        if (GLOBAL.equals(project))
            epoch = getGlobalEpoch();
        else
            epoch = NProjectManager.getInstance(config).getProject(project).getEpoch();
        if (epoch == null) {
            throw new IllegalStateException(String.format("Epoch of project %s does not exist", project));
        }
        return epoch.getEpochId();
    }

    public synchronized void forceUpdateEpoch(String project) {
        if (project.equals(EpochManager.GLOBAL)) {
            try {
                if (tryUpdateGlobalEpoch(Sets.newHashSet(), true)) {
                    currentEpochs.add(project);
                    if (!getGlobalEpoch().isMaintenanceMode()) {
                        schedulerEventBusFactory.post(new ProjectControlledNotifier(project));
                    }
                }
            } catch (Exception e) {
                logger.error("failed to update global epoch forcelly", e);
            }
        } else {
            ProjectInstance prj = NProjectManager.getInstance(config).getProject(project);
            Preconditions.checkNotNull(prj);
            if (updateProjectEpoch(prj, true)) {
                currentEpochs.add(project);
                if (!getGlobalEpoch().isMaintenanceMode()) {
                    schedulerEventBusFactory.post(new ProjectControlledNotifier(project));
                }
            }
        }
    }

    private void checkPrj(String project) {
        if (StringUtils.isEmpty(project)) {
            throw new IllegalStateException("Project should not be empty");
        }
    }

    public List<ProjectInstance> getOwnedProjects() {
        return NProjectManager.getInstance(config).listAllProjects().stream()
                .filter(p -> p.getEpoch().getCurrentEpochOwner().equals(identity)).collect(Collectors.toList());
    }

    public void shutdownOwnedProjects() {
        List<ProjectInstance> prjs = getOwnedProjects();
        for (ProjectInstance prj : prjs) {
            schedulerEventBusFactory.post(new ProjectEscapedNotifier(prj.getName()));
        }
    }

    public void startOwnedProjects() {
        List<ProjectInstance> prjs = getOwnedProjects();
        for (ProjectInstance prj : prjs) {
            schedulerEventBusFactory.post(new ProjectControlledNotifier(prj.getName()));
        }
    }
}
