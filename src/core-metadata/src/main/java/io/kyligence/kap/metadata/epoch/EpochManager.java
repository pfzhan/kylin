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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
import io.kyligence.kap.common.scheduler.EpochStartedNotifier;
import io.kyligence.kap.common.scheduler.EventBusFactory;
import io.kyligence.kap.common.scheduler.ProjectControlledNotifier;
import io.kyligence.kap.common.scheduler.ProjectEscapedNotifier;
import io.kyligence.kap.common.util.AddressUtil;
import io.kyligence.kap.metadata.project.NProjectManager;
import lombok.val;

public class EpochManager implements IKeep {
    private static final Logger logger = LoggerFactory.getLogger(EpochManager.class);

    public static EpochManager getInstance(KylinConfig config) {
        return config.getManager(EpochManager.class);
    }

    public static final String GLOBAL = "_global";

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

    public Boolean setMaintenanceMode(String reason) {
        if (isMaintenanceMode()) {
            return false;
        }
        for (Epoch epoch : epochStore.list()) {
            epoch.setCurrentEpochOwner(MAINTAIN_OWNER);
            epoch.setLastEpochRenewTime(Long.MAX_VALUE);
            epoch.setMaintenanceModeReason(reason);
            epochStore.saveOrUpdate(epoch);
        }

        return true;
    }

    public Boolean unsetMaintenanceMode(String reason) {
        if (!isMaintenanceMode()) {
            return false;
        }

        for (Epoch epoch : epochStore.list()) {
            epoch.setCurrentEpochOwner("");
            epoch.setLastEpochRenewTime(-1L);
            epoch.setMaintenanceModeReason(reason);
            epochStore.saveOrUpdate(epoch);
        }

        return true;
    }

    private List<String> getProjectsToMarkOwner() {
        return NProjectManager.getInstance(config).listAllProjects().stream().filter(p -> {
            Epoch epoch = epochStore.getEpoch(p.getName());
            if (!isEpochLegal(epoch) || epoch.getCurrentEpochOwner().equals(identity)) {
                return true;
            }
            return false;
        }).map(ProjectInstance::getName).collect(Collectors.toList());
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

    public boolean tryUpdateEpoch(String epochTarget, boolean force) {
        return tryUpdateEpoch(epochTarget, force, null);
    }

    private boolean tryUpdateEpoch(String epochTarget, boolean force, String maintenanceModeReason) {
        if (!force && checkInMaintenanceMode()) {
            return false;
        }
        try {
            Epoch epoch = epochStore.getEpoch(epochTarget);
            Epoch finalEpoch = getNewEpoch(epoch, force, epochTarget);
            if (finalEpoch == null) {
                return false;
            }

            finalEpoch.setMaintenanceModeReason(maintenanceModeReason);
            epochStore.saveOrUpdate(finalEpoch);

            return true;
        } catch (Exception e) {
            logger.error("Update " + epochTarget + " epoch failed.", e);
            return false;
        }
    }

    private Epoch getNewEpoch(Epoch epoch, boolean force, String epochTarget) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        if (!kylinConfig.getEpochCheckerEnabled()) {
            Epoch newEpoch = new Epoch(1L, epochTarget, identity, Long.MAX_VALUE, kylinConfig.getServerMode(), null,
                    0L);
            newEpoch.setMvcc(epoch == null ? 0 : epoch.getMvcc());
            return newEpoch;
        }
        if (epoch == null) {
            epoch = new Epoch(1L, epochTarget, identity, System.currentTimeMillis(), kylinConfig.getServerMode(), null,
                    0L);
        } else {
            if (!epoch.getCurrentEpochOwner().equals(identity)) {
                if (isEpochLegal(epoch) && !force)
                    return null;
                epoch.setEpochId(epoch.getEpochId() + 1);
            } else {
                if (!currentEpochs.contains(epochTarget)) {
                    epoch.setEpochId(epoch.getEpochId() + 1);
                }
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

    //when create a epochTarget, mark now
    public synchronized void updateEpoch(String epochTarget) throws Exception {
        if (checkInMaintenanceMode()) {
            return;
        }

        if (StringUtils.isEmpty(epochTarget)) {
            updateAllEpochs();
        }

        if (tryUpdateEpoch(epochTarget, false)) {
            currentEpochs.add(epochTarget);
            eventBusFactory.postAsync(new ProjectControlledNotifier(epochTarget));
        }
    }

    public Set<String> getAllLeadersByMode(String serverMode) {
        Set<String> leaders = epochStore.list().stream().filter(this::isEpochLegal).map(Epoch::getCurrentEpochOwner)
                .map(this::getHostAndPort).collect(Collectors.toSet());
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

    public synchronized boolean forceUpdateEpoch(String epochTarget) {
        return forceUpdateEpoch(epochTarget, null);
    }

    /**
     * only for maintainModeTool
     * @param epochTarget
     * @param maintenanceModeReason
     * @return
     */
    public synchronized boolean forceUpdateEpoch(String epochTarget, String maintenanceModeReason) {
        if (tryUpdateEpoch(epochTarget, true, maintenanceModeReason)) {
            currentEpochs.add(epochTarget);
            if (!isMaintenanceMode()) {
                eventBusFactory.postAsync(new ProjectControlledNotifier(epochTarget));
            }
            return true;
        }
        return false;
    }

    private void checkEpochTarget(String epochTarget) {
        if (StringUtils.isEmpty(epochTarget)) {
            throw new IllegalStateException("Project should not be empty");
        }
    }

    public Epoch getEpoch(String epochTarget) {
        return epochStore.getEpoch(epochTarget);
    }

    //for UT
    public void setIdentity(String newIdentity) {
        this.identity = newIdentity;
    }

    // when startup
    public void updateOwnedEpoch() {
        List<String> projects = NProjectManager.getInstance(KylinConfig.getInstanceFromEnv()).listAllProjects().stream()
                .map(ProjectInstance::getName).collect(Collectors.toList());

        epochStore.list().stream()
                .filter(epoch -> projects.contains(epoch.getEpochTarget())
                        || Objects.equals(epoch.getEpochTarget(), GLOBAL))
                .filter(epoch -> Objects.equals(getHostAndPort(epoch.getCurrentEpochOwner()),
                        AddressUtil.getLocalInstance()))
                .map(Epoch::getEpochTarget).forEach(this::forceUpdateEpoch);
    }

    public void deleteEpoch(String epochTarget) {
        epochStore.delete(epochTarget);
    }

    public Pair<Boolean, String> getMaintenanceModeDetail() {
        return getMaintenanceModeDetail(GLOBAL);
    }

    public Pair<Boolean, String> getMaintenanceModeDetail(String epochTarget) {
        Epoch epoch = epochStore.getEpoch(epochTarget);
        if (epoch != null && epoch.getCurrentEpochOwner().contains(":0000")) {
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
}
