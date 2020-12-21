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

import static io.kyligence.kap.common.util.AddressUtil.MAINTAIN_MODE_MOCK_PORT;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import io.kyligence.kap.guava20.shaded.common.collect.Sets;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.NamedThreadFactory;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.kyligence.kap.common.obf.IKeep;
import io.kyligence.kap.common.persistence.metadata.Epoch;
import io.kyligence.kap.common.persistence.metadata.EpochStore;
import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.common.scheduler.EpochStartedNotifier;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.ProjectControlledNotifier;
import io.kyligence.kap.common.scheduler.ProjectEscapedNotifier;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.resourcegroup.ResourceGroupManager;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.val;

public class EpochManager implements IKeep {
    private static final Logger logger = LoggerFactory.getLogger(EpochManager.class);

    public static EpochManager getInstance(KylinConfig config) {
        return config.getManager(EpochManager.class);
    }

    public static final String GLOBAL = UnitOfWork.GLOBAL_UNIT;

    private static final String MAINTAIN_OWNER;

    /**
     * store all project is owned by current node, 
     * but not in maintenance mode
     */
    private final Set<String> currentEpochs;

    private EpochStore epochStore;

    // called by reflection
    @SuppressWarnings("unused")
    static EpochManager newInstance(KylinConfig config) throws Exception {
        return new EpochManager(config);
    }

    // ============================================================================

    private KylinConfig config;
    private String identity;
    private EventBusFactory eventBusFactory;

    @Getter
    private final EpochUpdateManager epochUpdateManager;

    static {
        MAINTAIN_OWNER = AddressUtil.getMockPortAddress() + "|" + Long.MAX_VALUE;
    }

    private EpochManager(KylinConfig cfg) throws Exception {
        this.config = cfg;
        this.identity = EpochOrchestrator.getOwnerIdentity();
        eventBusFactory = EventBusFactory.getInstance();
        epochStore = EpochStore.getEpochStore(config);
        currentEpochs = Sets.newConcurrentHashSet();
        epochUpdateManager = new EpochUpdateManager();

    }

    @NoArgsConstructor
    public class EpochUpdateManager {
        private AtomicBoolean updateStarted = new AtomicBoolean(false);

        private Pair<HashSet<String>, List<String>> checkAndGetProjectEpoch() {
            if (checkInMaintenanceMode()) {
                return null;
            }
            val oriEpochs = Sets.newHashSet(currentEpochs);
            val projects = listProjectWithPermission();

            if (CollectionUtils.isEmpty(projects)) {
                logger.warn("project list is empty...");
                return null;
            }
            removeOutdatedCurrentEpoch(projects);

            return new Pair<>(oriEpochs, projects);
        }

        private void removeOutdatedCurrentEpoch(final List<String> projects) {
            if (CollectionUtils.isEmpty(currentEpochs)) {
                return;
            }

            val projectSets = new HashSet<>(projects);
            val outdatedProjects = new HashSet<>(Sets.difference(currentEpochs, projectSets));
            currentEpochs.retainAll(projectSets);

            if (CollectionUtils.isNotEmpty(outdatedProjects)) {
                logger.warn("outdated project list :{}:", String.join(",", outdatedProjects));
            }
        }

        synchronized void tryRenewOwnedEpochs() {
            logger.debug("Start renew owned epoch.........");

            //1.check and get project
            val epochSetProjectList = checkAndGetProjectEpoch();
            if (Objects.isNull(epochSetProjectList)) {
                return;
            }
            val oriEpochs = epochSetProjectList.getFirst();
            val projects = epochSetProjectList.getSecond();

            //2.only retain the project that is legal
            if (CollectionUtils.isNotEmpty(oriEpochs) && CollectionUtils.isNotEmpty(projects)) {
                oriEpochs.retainAll(projects);
            }

            if (CollectionUtils.isEmpty(oriEpochs)) {
                logger.info("current node own not project, end renew...");
                return;
            }

            //3.concurrent to update
            val renewExecutor = Executors.newFixedThreadPool(config.getRenewEpochWorkerPoolSize(),
                    new NamedThreadFactory("renew-epoch"));

            CountDownLatch latch = new CountDownLatch(oriEpochs.size());

            oriEpochs.forEach(project -> {
                renewExecutor.submit(() -> {
                    try {
                        if (!updateEpochByProject(project)) {
                            currentEpochs.remove(project);
                        }
                    } catch (Exception e) {
                        logger.error("update epoch project:{} error,", project, e);
                    } finally {
                        latch.countDown();
                    }

                });
            });

            try {
                latch.await();
            } catch (InterruptedException e) {
                logger.error("renew epoch is interrupted....", e);
                return;
            }

            notifierAfterUpdatedEpoch("renew", oriEpochs, currentEpochs);
            logger.debug("End renew owned epoch.........");
        }

        synchronized void tryUpdateAllEpochs(boolean updateOwnedEpoch) {
            logger.debug("Start updateAllEpochs:{}.........", updateOwnedEpoch);

            //1.check and get project
            val epochSetProjectList = checkAndGetProjectEpoch();
            if (Objects.isNull(epochSetProjectList)) {
                return;
            }
            val oriEpochs = epochSetProjectList.getFirst();
            val projects = epochSetProjectList.getSecond();

            //2.if update owned epoch only, remove all already project
            if (!updateOwnedEpoch && CollectionUtils.isNotEmpty(oriEpochs)) {
                projects.removeAll(oriEpochs);
            }

            if (CollectionUtils.isEmpty(projects)) {
                logger.debug("don't have more new project, end update...");
                return;
            }

            //3.update one by one
            Set<String> newEpochs = tryUpdateEpochByProjects(projects);

            //after update, the new origin epoch is oriEpochs - currentEpochs
            val newOriEpochs = new HashSet<>(Sets.difference(oriEpochs, currentEpochs));

            notifierAfterUpdatedEpoch("update", newOriEpochs, newEpochs);

            logger.debug("End updateAllEpochs:{}.........", updateOwnedEpoch);
        }

        private Set<String> tryUpdateEpochByProjects(final List<String> projects) {
            Set<String> newEpochs = new HashSet<>();

            if (CollectionUtils.isEmpty(projects)) {
                return newEpochs;
            }

            //random order
            Collections.shuffle(projects);

            projects.forEach(project -> {
                if (updateEpochByProject(project)) {
                    newEpochs.add(project);
                    currentEpochs.add(project);
                } else {
                    currentEpochs.remove(project);
                }
            });

            return newEpochs;
        }

        private boolean updateEpochByProject(String project) {
            boolean success = tryUpdateEpoch(project, false);
            return success || checkEpochOwner(project);
        }

        private void notifierAfterUpdatedEpoch(String updateTypeName, Set<String> oriEpochs, Set<String> newEpochs) {
            logger.debug("after {} new epoch size:{}, Project {} owned by {}", updateTypeName, newEpochs.size(),
                    String.join(",", newEpochs), identity);
            Collection<String> escapedProjects = new HashSet<>(Sets.difference(oriEpochs, newEpochs));
            for (String project : newEpochs) {
                eventBusFactory.postAsync(new ProjectControlledNotifier(project));
            }

            if (CollectionUtils.isNotEmpty(escapedProjects)) {
                for (String project : escapedProjects) {
                    eventBusFactory.postAsync(new ProjectEscapedNotifier(project));
                }

                logger.warn("{} escaped project:{}", updateTypeName, String.join(",", escapedProjects));
            }

            if (updateStarted.compareAndSet(false, true)) {
                eventBusFactory.postAsync(new EpochStartedNotifier());
            }
        }
    }

    private List<String> listProjectWithPermission() {
        List<String> projects = config.getEpochCheckerEnabled() ? getProjectsToMarkOwner()
                : NProjectManager.getInstance(config).listAllProjects().stream().map(ProjectInstance::getName)
                        .collect(Collectors.toList());
        projects.add(GLOBAL);
        return projects;
    }

    //for test
    public Epoch getGlobalEpoch() {
        return epochStore.getGlobalEpoch();
    }

    public boolean checkExpectedIsMaintenance(boolean expectedIsMaintenance) {
        return isMaintenanceMode() == expectedIsMaintenance;
    }

    private boolean switchMaintenanceMode(boolean expectedIsMaintenance, Consumer<Epoch> updateConsumer) {
        return updateEpochBatchTransaction(expectedIsMaintenance, () -> epochStore.list(), updateConsumer);
    }

    public boolean updateEpochBatchTransaction(boolean expectedIsMaintenance,
            @Nonnull Supplier<List<Epoch>> epochSupplier, @Nullable Consumer<Epoch> updateConsumer) {
        return epochStore.executeWithTransaction(() -> {
            if (!checkExpectedIsMaintenance(expectedIsMaintenance)) {
                return false;
            }

            val epochs = epochSupplier.get();

            if (Objects.nonNull(updateConsumer)) {
                epochs.forEach(updateConsumer);
            }

            epochStore.updateBatch(epochs);
            return true;
        });
    }

    public Boolean setMaintenanceMode(String reason) {

        return switchMaintenanceMode(false, epoch -> {
            epoch.setCurrentEpochOwner(MAINTAIN_OWNER);
            epoch.setLastEpochRenewTime(Long.MAX_VALUE);
            epoch.setMaintenanceModeReason(reason);
        });

    }

    public Boolean unsetMaintenanceMode(String reason) {

        return switchMaintenanceMode(true, epoch -> {
            epoch.setCurrentEpochOwner("");
            epoch.setLastEpochRenewTime(-1L);
            epoch.setMaintenanceModeReason(reason);
        });
    }

    private List<String> getProjectsToMarkOwner() {
        return NProjectManager.getInstance(config).listAllProjects().stream()
                .filter(p -> currentInstanceHasPermissionToOwn(p.getName(), false)).map(ProjectInstance::getName)
                .collect(Collectors.toList());
    }

    /**
     *
     * the method only update epoch'meta,
     * will not post ProjectControlledNotifier event
     * so it can be safely used by tool
     *
     * @param projects projects need to be updated or inserted
     * @param skipCheckMaintMode if true, should not check maintenance mode status
     * @param maintenanceModeReason
     * @param expectedIsMaintenance the expected maintenance mode
     * @return
     */
    public boolean tryForceInsertOrUpdateEpochBatchTransaction(List<String> projects, boolean skipCheckMaintMode,
            String maintenanceModeReason, boolean expectedIsMaintenance) {

        return epochStore.executeWithTransaction(() -> {
            if ((!skipCheckMaintMode && !checkExpectedIsMaintenance(expectedIsMaintenance))
                    || CollectionUtils.isEmpty(projects)) {
                return false;
            }

            val epochList = epochStore.list();

            //epochs need to be updated
            val needUpdateProjectSet = epochList.stream().map(Epoch::getEpochTarget).collect(Collectors.toSet());
            if (CollectionUtils.isNotEmpty(needUpdateProjectSet)) {
                val needUpdateEpochList = epochList.stream()
                        .filter(epoch -> needUpdateProjectSet.contains(epoch.getEpochTarget())).map(epochTemp -> {
                            Pair<Epoch, Epoch> pair = oldEpoch2NewEpoch(epochTemp, epochTemp.getEpochTarget(), true,
                                    maintenanceModeReason);
                            if (Objects.nonNull(pair)) {
                                return pair.getSecond();
                            }
                            return null;
                        }).filter(Objects::nonNull).collect(Collectors.toList());
                //batch update
                epochStore.updateBatch(needUpdateEpochList);
            }

            //epoch need to be inserted
            val needInsertProjectSet = Sets.difference(new HashSet<>(projects), needUpdateProjectSet);
            if (CollectionUtils.isNotEmpty(needInsertProjectSet)) {
                val needInsertEpochList = needInsertProjectSet.stream().map(project -> {
                    Pair<Epoch, Epoch> pair = oldEpoch2NewEpoch(null, project, true, maintenanceModeReason);
                    if (Objects.nonNull(pair)) {
                        return pair.getSecond();
                    }
                    return null;
                }).filter(Objects::nonNull).collect(Collectors.toList());
                epochStore.insertBatch(needInsertEpochList);
            }

            //update current node's epoch
            currentEpochs.addAll(projects);
            return true;
        });

    }

    @Nullable
    private Pair<Epoch, Epoch> oldEpoch2NewEpoch(@Nullable Epoch oldEpoch, @Nonnull String epochTarget, boolean force,
            String maintenanceModeReason) {
        Epoch finalEpoch = getNewEpoch(oldEpoch, force, epochTarget);
        if (finalEpoch == null) {
            return null;
        }

        finalEpoch.setMaintenanceModeReason(maintenanceModeReason);
        return new Pair<>(oldEpoch, finalEpoch);
    }

    public boolean tryUpdateEpoch(String epochTarget, boolean force) {
        if (!force && checkInMaintenanceMode()) {
            return false;
        }
        try {
            Epoch epoch = epochStore.getEpoch(epochTarget);
            Pair<Epoch, Epoch> oldNewEpochPair = oldEpoch2NewEpoch(epoch, epochTarget, force, null);

            //current epoch already has owner and not to force
            if (Objects.isNull(oldNewEpochPair)) {
                return false;
            }
            insertOrUpdateEpoch(oldNewEpochPair.getSecond());

            if (Objects.nonNull(oldNewEpochPair.getFirst())
                    && !Objects.equals(oldNewEpochPair.getFirst().getCurrentEpochOwner(),
                            oldNewEpochPair.getSecond().getCurrentEpochOwner())) {
                logger.debug("Epoch {} changed from {} to {}", epochTarget,
                        oldNewEpochPair.getFirst().getCurrentEpochOwner(),
                        oldNewEpochPair.getSecond().getCurrentEpochOwner());
            }

            return true;
        } catch (Exception e) {
            logger.error("Update " + epochTarget + " epoch failed.", e);
            return false;
        }
    }

    /**
     * if epoch's target is not in meta data,insert new one,
     * otherwise update it
     * @param epoch
     */
    public void insertOrUpdateEpoch(Epoch epoch) {
        if (Objects.isNull(epoch)) {
            return;
        }
        epochStore.executeWithTransaction(() -> {

            if (Objects.isNull(getEpoch(epoch.getEpochTarget()))) {
                epochStore.insert(epoch);
            } else {
                epochStore.update(epoch);
            }

            return null;
        });

    }

    private Epoch getNewEpoch(@Nullable Epoch epoch, boolean force, @Nonnull String epochTarget) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.getEpochCheckerEnabled()) {
            Epoch newEpoch = new Epoch(1L, epochTarget, identity, Long.MAX_VALUE, kylinConfig.getServerMode(), null,
                    0L);
            newEpoch.setMvcc(epoch == null ? 0 : epoch.getMvcc());
            return newEpoch;
        }
        if (!currentInstanceHasPermissionToOwn(epochTarget, force)) {
            return null;
        }
        if (epoch == null) {
            epoch = new Epoch(1L, epochTarget, identity, System.currentTimeMillis(), kylinConfig.getServerMode(), null,
                    0L);
        } else {
            if (!epoch.getCurrentEpochOwner().equals(identity)) {
                if (isEpochLegal(epoch) && !force) {
                    return null;
                }
                epoch.setEpochId(epoch.getEpochId() + 1);
            } else if (!currentEpochs.contains(epochTarget)) {
                epoch.setEpochId(epoch.getEpochId() + 1);
            }
            epoch.setServerMode(kylinConfig.getServerMode());
            epoch.setLastEpochRenewTime(System.currentTimeMillis());
            epoch.setCurrentEpochOwner(identity);
        }
        return epoch;
    }

    public synchronized void updateAllEpochs() {
        epochUpdateManager.tryUpdateAllEpochs(true);
    }

    public boolean checkEpochOwner(String epochTarget) {
        Epoch epoch = epochStore.getEpoch(epochTarget);
        return isEpochLegal(epoch) && epoch.getCurrentEpochOwner().equals(identity);
    }

    public void updateEpochWithNotifier(String epochTarget, boolean force) {
        if (tryUpdateEpoch(epochTarget, force)) {
            currentEpochs.add(epochTarget);
            eventBusFactory.postAsync(new ProjectControlledNotifier(epochTarget));
        }
    }

    private boolean currentInstanceHasPermissionToOwn(String epochTarget, boolean force) {
        // if force, no need to check resource group, eg: switch maintenance mode.
        if (force) {
            return true;
        }
        ResourceGroupManager rgManager = ResourceGroupManager.getInstance(KylinConfig.getInstanceFromEnv());
        return rgManager.instanceHasPermissionToOwnEpochTarget(epochTarget, AddressUtil.getLocalInstance());
    }

    private boolean isEpochLegal(Epoch epoch) {
        if (epoch == null) {
            logger.debug("Get null epoch");
            return false;
        } else if (StringUtils.isEmpty(epoch.getCurrentEpochOwner())) {
            logger.debug("Epoch {}'s owner is empty", epoch);
            return false;
        } else if (System.currentTimeMillis() - epoch.getLastEpochRenewTime() > config.getEpochExpireTimeSecond()
                * 1000) {
            logger.warn("Epoch {}'s last renew time is expired. Current time is {}, expiredTime is {}", epoch,
                    System.currentTimeMillis(), config.getEpochExpireTimeSecond());
            return false;
        }

        ResourceGroupManager rgManager = ResourceGroupManager.getInstance(config);
        String epochServer = getHostAndPort(epoch.getCurrentEpochOwner());
        if (!rgManager.instanceHasPermissionToOwnEpochTarget(epoch.getEpochTarget(), epochServer)) {
            logger.debug("Epoch {}'s owner is not in build request type resource group.", epoch);
            return false;
        }
        return true;
    }

    public String getEpochOwner(String epochTarget) {
        checkEpochTarget(epochTarget);
        Epoch epoch = epochStore.getEpoch(epochTarget);
        if (isEpochLegal(epoch)) {
            return getHostAndPort(epoch.getCurrentEpochOwner());
        } else {
            return null;
        }
    }

    public String getHostAndPort(String owner) {
        return owner.split("\\|")[0];
    }

    //ensure only one epochTarget thread running
    public boolean checkEpochId(long epochId, String epochTarget) {
        return getEpochId(epochTarget) == epochId;
    }

    public long getEpochId(String epochTarget) {
        checkEpochTarget(epochTarget);
        Epoch epoch = epochStore.getEpoch(epochTarget);
        if (epoch == null) {
            throw new IllegalStateException(
                    String.format(Locale.ROOT, "Epoch of project %s does not exist", epochTarget));
        }
        return epoch.getEpochId();
    }

    private void checkEpochTarget(String epochTarget) {
        if (StringUtils.isEmpty(epochTarget)) {
            throw new IllegalStateException("Project should not be empty");
        }
    }

    public Epoch getEpoch(String epochTarget) {
        return epochStore.getEpoch(epochTarget);
    }

    public void setIdentity(String newIdentity) {
        this.identity = newIdentity;
    }

    public void deleteEpoch(String epochTarget) {
        epochStore.delete(epochTarget);
    }

    public Pair<Boolean, String> getMaintenanceModeDetail() {
        return getMaintenanceModeDetail(GLOBAL);
    }

    public Pair<Boolean, String> getMaintenanceModeDetail(String epochTarget) {
        Epoch epoch = epochStore.getEpoch(epochTarget);
        if (epoch != null && epoch.getCurrentEpochOwner().contains(":" + MAINTAIN_MODE_MOCK_PORT)) {
            return Pair.newPair(true, epoch.getMaintenanceModeReason());
        }

        return Pair.newPair(false, null);
    }

    public boolean isMaintenanceMode() {
        return getMaintenanceModeDetail().getFirst();
    }

    private boolean checkInMaintenanceMode() {
        if (isMaintenanceMode()) {
            logger.debug("System is currently undergoing maintenance. Abort updating Epochs");
            return true;
        }
        return false;
    }

    // when shutdown or meta data is inconsistent
    public void releaseOwnedEpochs() {
        logger.info("Release owned epochs");
        epochStore.executeWithTransaction(() -> {
            val epochs = epochStore.list().stream()
                    .filter(epoch -> Objects.equals(epoch.getCurrentEpochOwner(), identity))
                    .collect(Collectors.toList());
            epochs.forEach(epoch -> {
                epoch.setCurrentEpochOwner("");
                epoch.setLastEpochRenewTime(-1L);
            });

            epochStore.updateBatch(epochs);
            return null;
        });
    }
}
