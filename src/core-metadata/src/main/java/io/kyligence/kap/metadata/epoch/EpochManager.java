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
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Sets;

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
import lombok.val;

public class EpochManager implements IKeep {
    private static final Logger logger = LoggerFactory.getLogger(EpochManager.class);

    public static EpochManager getInstance(KylinConfig config) {
        return config.getManager(EpochManager.class);
    }

    public static final String GLOBAL = UnitOfWork.GLOBAL_UNIT;

    private static final String MAINTAIN_OWNER;

    private Set<String> currentEpochs = Sets.newCopyOnWriteArraySet();

    private boolean started = false;

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

    static {
        MAINTAIN_OWNER = AddressUtil.getMockPortAddress() + "|" + Long.MAX_VALUE;
    }

    private EpochManager(KylinConfig cfg) throws Exception {
        this.config = cfg;
        this.identity = EpochOrchestrator.getOwnerIdentity();
        eventBusFactory = EventBusFactory.getInstance();
        epochStore = EpochStore.getEpochStore(config);
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
        return NProjectManager.getInstance(config).listAllProjects()
                .stream()
                .filter(p -> currentInstanceHasPermissionToOwn(p.getName(), false))
                .map(ProjectInstance::getName)
                .collect(Collectors.toList());
    }

    private Set<String> tryUpdateAllEpoch() {
        Set<String> newEpochs = new HashSet<>();
        List<String> prjs = config.getEpochCheckerEnabled() ? getProjectsToMarkOwner()
                : NProjectManager.getInstance(config).listAllProjects().stream().map(ProjectInstance::getName)
                        .collect(Collectors.toList());

        prjs.add(GLOBAL);

        for (String prj : prjs) {
            boolean success = tryUpdateEpoch(prj, false);
            if (success || checkEpochOwner(prj)) {
                newEpochs.add(prj);
            }
        }
        return newEpochs;
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
                        .filter(epoch -> needUpdateProjectSet.contains(epoch.getEpochTarget()))
                        .map(epochTemp -> {
                            Pair<Epoch, Epoch> pair = oldEpoch2NewEpoch(epochTemp, epochTemp.getEpochTarget(),
                                    true, maintenanceModeReason);
                            if (Objects.nonNull(pair)) {
                                return pair.getSecond();
                            }
                            return null;
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                //batch update
                epochStore.updateBatch(needUpdateEpochList);
            }

            //epoch need to be inserted
            val needInsertProjectSet = Sets.difference(new HashSet<>(projects), needUpdateProjectSet);
            if (CollectionUtils.isNotEmpty(needInsertProjectSet)) {
                val needInsertEpochList = needInsertProjectSet.stream()
                        .map(project -> {
                            Pair<Epoch, Epoch> pair = oldEpoch2NewEpoch(null, project, true, maintenanceModeReason);
                            if (Objects.nonNull(pair)) {
                                return pair.getSecond();
                            }
                            return null;
                        })
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                epochStore.insertBatch(needInsertEpochList);
            }

            //update current node's epoch
            currentEpochs.addAll(projects);
            return true;
        });

    }

    @Nullable
    private Pair<Epoch, Epoch> oldEpoch2NewEpoch(@Nullable Epoch oldEpoch, @Nonnull String epochTarget, boolean force, String maintenanceModeReason) {
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
        logger.debug("Start updateAllEpochs.........");
        if (checkInMaintenanceMode()) {
            return;
        }
        val oriEpochs = Sets.newHashSet(currentEpochs);
        Set<String> newEpochs = tryUpdateAllEpoch();
        logger.debug("Project [" + String.join(",", newEpochs) + "] owned by " + identity);
        Collection<String> retainPrjs = CollectionUtils.retainAll(oriEpochs, newEpochs);
        Collection<String> escapedProjects = CollectionUtils.removeAll(oriEpochs, retainPrjs);
        for (String prj : newEpochs) {
            eventBusFactory.postAsync(new ProjectControlledNotifier(prj));
        }

        for (String prj : escapedProjects) {
            eventBusFactory.postAsync(new ProjectEscapedNotifier(prj));
        }

        if (!started) {
            started = true;
            eventBusFactory.postAsync(new EpochStartedNotifier());
        }
        logger.debug("End updateAllEpochs.........");
        currentEpochs = newEpochs;
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
            logger.debug("Epoch {}'s last renew time is expired. Current time is {}, expiredTime is {}", epoch,
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
            throw new IllegalStateException(String.format("Epoch of project %s does not exist", epochTarget));
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

    // when shutdown
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
