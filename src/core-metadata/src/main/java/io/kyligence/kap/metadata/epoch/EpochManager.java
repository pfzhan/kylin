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

import com.google.common.collect.Sets;
import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EpochStartedNotifier;
import io.kyligence.kap.common.scheduler.ProjectEscapedNotifier;
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.JsonSerializer;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.persistence.Serializer;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.kylin.common.persistence.ResourceStore.GLOBAL_EPOCH;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import io.kyligence.kap.common.scheduler.ProjectControlledNotifier;


public class EpochManager implements IKeep {
    private static final Logger logger = LoggerFactory.getLogger(EpochManager.class);

    private static final Serializer<Epoch> EPOCH_SERIALIZER = new JsonSerializer<Epoch>(Epoch.class);

    public static EpochManager getInstance(KylinConfig config) {
        return config.getManager(EpochManager.class);
    }

    public static final String GLOBAL = "_global";

    public static ExecutorService pool;

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

    private EpochManager(KylinConfig cfg) {
        this.config = cfg;
        this.identity = EpochOrchestrator.getOwnerIdentity();
        schedulerEventBusFactory = SchedulerEventBusFactory.getInstance(config);
        pool = new ThreadPoolExecutor(5, 10, 60, TimeUnit.SECONDS, new LinkedBlockingDeque<>());
    }

    private Epoch getGlobalEpoch() {
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).getResource(GLOBAL_EPOCH, EPOCH_SERIALIZER);
    }

    private boolean updateGlobalEpoch() {
        return UnitOfWork.doInTransactionWithRetry(() -> {
            Epoch epoch = getNewEpoch(getGlobalEpoch());
            if (epoch == null) {
                return false;
            }
            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).checkAndPutResource(GLOBAL_EPOCH, epoch, EPOCH_SERIALIZER);
            return true;
        }, GLOBAL, 1);
    }

    private List<ProjectInstance> getProjectsToMarkOwner() {
        List<ProjectInstance> prjs = NProjectManager.getInstance(config).listAllProjects();
        return prjs.stream().filter(p -> !isEpochLegal(p.getEpoch()) || p.getEpoch().getCurrentEpochOwner().equals(identity)).collect(Collectors.toList());
    }


    private CountDownLatch tryUpdatePrjEpochs(Set<String> newEpochs) {
        List<ProjectInstance> prjs = getProjectsToMarkOwner();
        val cdl = new CountDownLatch(prjs.size());
        for (ProjectInstance prj : prjs) {
            pool.execute(new Thread(() -> {
                try {
                    if (updateProjectEpoch(prj)) newEpochs.add(prj.getName());
                } finally {
                    cdl.countDown();
                }
            }));
        }
        return cdl;
    }


    public boolean updateProjectEpoch(ProjectInstance prj) {
        try {
            return UnitOfWork.doInTransactionWithRetry(() -> {
                val kylinConfig = KylinConfig.getInstanceFromEnv();
                ProjectInstance project = NProjectManager.getInstance(kylinConfig).getProject(prj.getName());
                if (project == null) {
                    return false;
                }
                Epoch finalEpoch = getNewEpoch(project.getEpoch());
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

    public void tryUpdateGlobalEpoch(Set<String> newEpochs) {
        try {
            if (updateGlobalEpoch()) {
                newEpochs.add(GLOBAL);
            }
        } catch (Exception e) {
            logger.error("Try to update global epoch failed.", e);
        }
    }

    private Epoch getNewEpoch(Epoch epoch) {
        if (epoch == null) {
            epoch = new Epoch(1L, identity, System.currentTimeMillis());
        } else {
            if (!epoch.getCurrentEpochOwner().equals(identity)) {
                if (isEpochLegal(epoch)) return null;
                epoch.setEpochId(epoch.getEpochId() + 1);
            }
            epoch.setLastEpochRenewTime(System.currentTimeMillis());
            epoch.setCurrentEpochOwner(identity);
        }
        return epoch;
    }

    public synchronized void updateAllEpochs() {
        logger.info("start updateAllEpochs.........");
        val oriEpochs = Sets.newHashSet(currentEpochs);
        Set<String> newEpochs = Sets.newCopyOnWriteArraySet();
        tryUpdateGlobalEpoch(newEpochs);
        CountDownLatch cdl = tryUpdatePrjEpochs(newEpochs);
        try {
            cdl.await();
        } catch (InterruptedException e) {
            logger.error("Unexpected things happened" + e);
            return;
        }
        logger.info("Project [" + String.join(",", newEpochs) + "] owned by " + identity);
        Collection<String> retainPrjs = CollectionUtils.retainAll(oriEpochs, newEpochs);
        Collection<String> newProjects = CollectionUtils.removeAll(newEpochs, retainPrjs);
        Collection<String> escapedProjects = CollectionUtils.removeAll(oriEpochs, retainPrjs);
        for (String prj : newProjects) {
            schedulerEventBusFactory.post(new ProjectControlledNotifier(prj));
        }

        for (String prj : escapedProjects) {
            schedulerEventBusFactory.post(new ProjectEscapedNotifier(prj));
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
        if (StringUtils.isEmpty(project))
            throw new IllegalStateException("Project should not be empty");
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
    public synchronized void updateEpoch(String project) {
        if (StringUtils.isEmpty(project))
            updateAllEpochs();
        if (project.equals(GLOBAL))
            tryUpdateGlobalEpoch(Sets.newCopyOnWriteArraySet());
        ProjectInstance prj = NProjectManager.getInstance(config).getProject(project);
        if (prj == null) {
            throw new IllegalStateException("Project " + project + " does not exist");
        }
        if (updateProjectEpoch(prj)) {
            schedulerEventBusFactory.post(new ProjectControlledNotifier(project));
        }
    }

    public Set<String> getAllLeaders() {
        NProjectManager prjMgr = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv());
        Set<String> leaders = Sets.newHashSet();
        prjMgr.listAllProjects().stream().map(ProjectInstance::getEpoch)
                .filter(epoch -> isEpochLegal(epoch))
                .map(Epoch::getCurrentEpochOwner).map(s -> getHostAndPort(s)).collect(Collectors.toSet());
        Epoch globalEpoch = getGlobalEpoch();
        if (isEpochLegal(globalEpoch)) {
            leaders.add(getHostAndPort(globalEpoch.getCurrentEpochOwner()));
        }
        return leaders;
    }

    public boolean isEpochLegal(Epoch epoch) {
        return epoch != null && StringUtils.isNotEmpty(epoch.getCurrentEpochOwner()) && System.currentTimeMillis() - epoch.getLastEpochRenewTime() <= config.getEpochExpireTimeSecond() * 1000;
    }

    public String getEpochOwner(String project) {
        if (StringUtils.isEmpty(project))
            throw new IllegalArgumentException("Project should not be empty");
        Epoch epoch;
        if (project.equals(GLOBAL)) {
            epoch = getGlobalEpoch();
        } else {
            NProjectManager projectManager = NProjectManager.getInstance(config);
            ProjectInstance prj = projectManager.getProject(project);
            if (prj == null)
                throw new IllegalArgumentException("Project " + project + " does not exist");
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
        if (StringUtils.isEmpty(project)) {
            throw new IllegalStateException("Project should not be empty");
        }
        Epoch epoch = null;
        if (GLOBAL.equals(project))
            epoch = getGlobalEpoch();
        else
            epoch = NProjectManager.getInstance(config).getProject(project).getEpoch();
        if (epoch == null) {
            throw new IllegalStateException("Epoch of project " + project + " does not exist");
        }
        return epoch.getEpochId();
    }
}
