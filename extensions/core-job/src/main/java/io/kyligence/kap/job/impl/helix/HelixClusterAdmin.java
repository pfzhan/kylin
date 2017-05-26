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

package io.kyligence.kap.job.impl.helix;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.helix.HelixAdmin;
import org.apache.helix.HelixManager;
import org.apache.helix.HelixManagerFactory;
import org.apache.helix.InstanceType;
import org.apache.helix.LiveInstanceChangeListener;
import org.apache.helix.NotificationContext;
import org.apache.helix.api.id.StateModelDefId;
import org.apache.helix.controller.HelixControllerMain;
import org.apache.helix.manager.zk.ZKHelixAdmin;
import org.apache.helix.model.ExternalView;
import org.apache.helix.model.IdealState;
import org.apache.helix.model.InstanceConfig;
import org.apache.helix.model.LiveInstance;
import org.apache.helix.model.StateModelDefinition;
import org.apache.helix.tools.StateModelConfigGenerator;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.StringUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Administrator of Kylin cluster
 */
public class HelixClusterAdmin {

    public static final String RESOURCE_NAME_JOB_ENGINE = "Resource_JobEngine";

    public static final String MODEL_LEADER_STANDBY = "LeaderStandby";
    public static final String TAG_JOB_ENGINE = "Tag_JobEngine";

    private static ConcurrentMap<KylinConfig, HelixClusterAdmin> instanceMaps = Maps.newConcurrentMap();
    private HelixManager participantManager;
    private HelixManager controllerManager;

    private final KylinConfig kylinConfig;

    private static final Logger logger = LoggerFactory.getLogger(HelixClusterAdmin.class);
    private String zkAddress;
    private final HelixAdmin admin;
    private final String clusterName;

    private HelixClusterAdmin(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;

        KapConfig kapConfig = KapConfig.wrap(kylinConfig);
        if (kapConfig.getHelixZookeeperAddress() != null) {
            this.zkAddress = kapConfig.getHelixZookeeperAddress();
        } else {
            throw new IllegalArgumentException("no 'kap.job.helix.zookeeper-address' set in kylin.properties");
        }

        this.clusterName = kylinConfig.getClusterName();
        this.admin = new ZKHelixAdmin(zkAddress);
    }

    public void start() {
        initCluster();
        final String instanceName = getCurrentInstanceName();

        // use the tag to mark node's role.
        final List<String> instanceTags = Lists.newArrayList();
        instanceTags.add(HelixClusterAdmin.TAG_JOB_ENGINE);

        addInstance(instanceName, instanceTags);
        startInstance(instanceName);

        rebalanceWithTag(RESOURCE_NAME_JOB_ENGINE, TAG_JOB_ENGINE);

        startController();
    }

    /**
     * Initiate the cluster, adding state model definitions and resource definitions
     */
    protected void initCluster() {
        admin.addCluster(clusterName, false);
        if (admin.getStateModelDef(clusterName, MODEL_LEADER_STANDBY) == null) {
            admin.addStateModelDef(clusterName, MODEL_LEADER_STANDBY,
                    new StateModelDefinition(StateModelConfigGenerator.generateConfigForLeaderStandby()));
        }

        // add job engine as a resource, 1 partition
        if (!admin.getResourcesInCluster(clusterName).contains(HelixClusterAdmin.RESOURCE_NAME_JOB_ENGINE)) {
            admin.addResource(clusterName, HelixClusterAdmin.RESOURCE_NAME_JOB_ENGINE, 1, MODEL_LEADER_STANDBY,
                    IdealState.RebalanceMode.FULL_AUTO.name());
        }

    }

    /**
     * Start the instance and register the state model factory
     *
     * @param instanceName
     * @throws Exception
     */
    protected void startInstance(String instanceName) {
        participantManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT,
                zkAddress);
        participantManager.getStateMachineEngine().registerStateModelFactory(StateModelDefId.from(MODEL_LEADER_STANDBY),
                new LeaderStandbyStateModelFactory(this.kylinConfig));
        try {
            participantManager.connect();
            participantManager.addLiveInstanceChangeListener(new KylinClusterLiveInstanceChangeListener());
        } catch (Exception e) {
            throw new IllegalStateException("failed to connect with Helix server", e);
        }

    }

    /**
     * Rebalance the resource with the tags
     *
     */
    protected void rebalanceWithTag(String resourceName, String tag) {
        admin.rebalance(clusterName, resourceName, 2, null, tag);
    }

    /**
     * Start an embedded helix controller
     */
    protected void startController() {
        controllerManager = HelixControllerMain.startHelixController(zkAddress, clusterName, "controller",
                HelixControllerMain.STANDALONE);
    }

    public void stop() {
        if (participantManager != null) {
            participantManager.disconnect();
            participantManager = null;
        }

        if (controllerManager != null) {
            controllerManager.disconnect();
            controllerManager = null;
        }
    }

    public String getInstanceState(String resourceName) {
        String instanceName = this.getCurrentInstanceName();
        final ExternalView resourceExternalView = admin.getResourceExternalView(clusterName, resourceName);
        if (resourceExternalView == null) {
            logger.warn("fail to get ExternalView, clusterName:" + clusterName + " resourceName:" + resourceName);
            return "ERROR";
        }
        final Set<String> partitionSet = resourceExternalView.getPartitionSet();
        if (partitionSet.size() == 0) {
            return "ERROR";
        }
        final Map<String, String> stateMap = resourceExternalView.getStateMap(partitionSet.iterator().next());
        if (stateMap.containsKey(instanceName)) {
            return stateMap.get(instanceName);
        } else {
            logger.warn("fail to get state, clusterName:" + clusterName + " resourceName:" + resourceName + " instance:"
                    + instanceName);
            return "ERROR";
        }
    }

    /**
     * Check whether current kylin instance is in the leader role
     *
     * @return
     */
    public boolean isLeaderRole(String resourceName) {
        final String instanceState = getInstanceState(resourceName);
        logger.debug("instance state: " + instanceState);
        if ("LEADER".equalsIgnoreCase(instanceState)) {
            return true;
        }

        return false;
    }

    /**
     * Add instance to cluster, with a tag list
     *
     * @param instanceName should be unique in format: hostName_port
     * @param tags
     */
    public void addInstance(String instanceName, List<String> tags) {
        final String hostname = instanceName.substring(0, instanceName.lastIndexOf("_"));
        final String port = instanceName.substring(instanceName.lastIndexOf("_") + 1);
        InstanceConfig instanceConfig = new InstanceConfig(instanceName);
        instanceConfig.setHostName(hostname);
        instanceConfig.setPort(port);
        if (tags != null) {
            for (String tag : tags) {
                instanceConfig.addTag(tag);
            }
        }

        if (admin.getInstancesInCluster(clusterName).contains(instanceName)) {
            admin.dropInstance(clusterName, instanceConfig);
        }
        admin.addInstance(clusterName, instanceConfig);
    }

    public static HelixClusterAdmin getInstance(KylinConfig kylinConfig) {
        Preconditions.checkNotNull(kylinConfig);
        instanceMaps.putIfAbsent(kylinConfig, new HelixClusterAdmin(kylinConfig));
        return instanceMaps.get(kylinConfig);
    }

    public String getCurrentInstanceName() {
        final String restAddress = KapConfig.wrap(kylinConfig).getHelixRestAddress();
        if (StringUtils.isEmpty(restAddress)) {
            throw new RuntimeException(
                    "There is no kap.job.helix.host-address set in System property and kylin.properties;");
        }

        final String hostname = Preconditions.checkNotNull(restAddress.substring(0, restAddress.lastIndexOf(":")),
                "failed to get HostName of this server");
        final String port = Preconditions.checkNotNull(restAddress.substring(restAddress.lastIndexOf(":") + 1),
                "failed to get port of this server");
        return hostname + "_" + port;
    }

    /**
     * Listen to the cluster's event, update "kylin.server.cluster-servers" to the live instances.
     */
    class KylinClusterLiveInstanceChangeListener implements LiveInstanceChangeListener {
        @Override
        public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
            List<String> instanceRestAddresses = Lists.newArrayList();
            for (LiveInstance liveInstance : liveInstances) {
                String instanceName = liveInstance.getInstanceName();
                int indexOfUnderscore = instanceName.lastIndexOf("_");
                instanceRestAddresses.add(instanceName.substring(0, indexOfUnderscore) + ":"
                        + instanceName.substring(indexOfUnderscore + 1));
            }
            if (instanceRestAddresses.size() > 0) {
                String restServersInCluster = StringUtil.join(instanceRestAddresses, ",");
                String serverMode = isLeaderRole(HelixClusterAdmin.RESOURCE_NAME_JOB_ENGINE) ? "all" : "query";
                logger.info("kylin.server.cluster-servers update to " + restServersInCluster);
                logger.info("kylin.server.mode update to " + serverMode);

                kylinConfig.setProperty("kylin.server.cluster-servers", restServersInCluster);
                kylinConfig.setProperty("kylin.server.mode", serverMode);
                System.setProperty("kylin.server.cluster-servers", restServersInCluster);
            }
        }
    }
}
