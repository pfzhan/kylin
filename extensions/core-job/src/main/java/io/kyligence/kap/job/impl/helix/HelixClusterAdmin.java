/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/
package io.kyligence.kap.job.impl.helix;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
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
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.Broadcaster;
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

        if (kylinConfig.getZookeeperAddress() != null) {
            this.zkAddress = kylinConfig.getZookeeperAddress();
        } else {
            throw new IllegalArgumentException("no 'kylin.zookeeper.address' set in kylin.properties");
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
            admin.addStateModelDef(clusterName, MODEL_LEADER_STANDBY, new StateModelDefinition(StateModelConfigGenerator.generateConfigForLeaderStandby()));
        }

        // add job engine as a resource, 1 partition
        if (!admin.getResourcesInCluster(clusterName).contains(HelixClusterAdmin.RESOURCE_NAME_JOB_ENGINE)) {
            admin.addResource(clusterName, HelixClusterAdmin.RESOURCE_NAME_JOB_ENGINE, 1, MODEL_LEADER_STANDBY, IdealState.RebalanceMode.FULL_AUTO.name());
        }

    }

    /**
     * Start the instance and register the state model factory
     *
     * @param instanceName
     * @throws Exception
     */
    protected void startInstance(String instanceName) {
        participantManager = HelixManagerFactory.getZKHelixManager(clusterName, instanceName, InstanceType.PARTICIPANT, zkAddress);
        participantManager.getStateMachineEngine().registerStateModelFactory(StateModelDefId.from(MODEL_LEADER_STANDBY), new LeaderStandbyStateModelFactory(this.kylinConfig));
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
        controllerManager = HelixControllerMain.startHelixController(zkAddress, clusterName, "controller", HelixControllerMain.STANDALONE);
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
            logger.warn("fail to get state, clusterName:" + clusterName + " resourceName:" + resourceName + " instance:" + instanceName);
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
        final String restAddress = kylinConfig.getRestAddress();
        if (StringUtils.isEmpty(restAddress)) {
            throw new RuntimeException("There is no kylin.rest.address set in System property and kylin.properties;");
        }

        final String hostname = Preconditions.checkNotNull(restAddress.substring(0, restAddress.lastIndexOf(":")), "failed to get HostName of this server");
        final String port = Preconditions.checkNotNull(restAddress.substring(restAddress.lastIndexOf(":") + 1), "failed to get port of this server");
        return hostname + "_" + port;
    }

    /**
     * Listen to the cluster's event, update "kylin.rest.servers" to the live instances.
     */
    class KylinClusterLiveInstanceChangeListener implements LiveInstanceChangeListener {
        @Override
        public void onLiveInstanceChange(List<LiveInstance> liveInstances, NotificationContext changeContext) {
            List<String> instanceRestAddresses = Lists.newArrayList();
            for (LiveInstance liveInstance : liveInstances) {
                String instanceName = liveInstance.getInstanceName();
                int indexOfUnderscore = instanceName.lastIndexOf("_");
                instanceRestAddresses.add(instanceName.substring(0, indexOfUnderscore) + ":" + instanceName.substring(indexOfUnderscore + 1));
            }
            if (instanceRestAddresses.size() > 0) {
                String restServersInCluster = StringUtil.join(instanceRestAddresses, ",");
                String serverMode = isLeaderRole(HelixClusterAdmin.RESOURCE_NAME_JOB_ENGINE) ? "all" : "query";
                logger.info("kylin.rest.servers update to " + restServersInCluster);
                logger.info("kylin.server.mode update to " + serverMode);

                kylinConfig.setProperty("kylin.rest.servers", restServersInCluster);
                kylinConfig.setProperty("kylin.server.mode", serverMode);
                System.setProperty("kylin.rest.servers", restServersInCluster);
                Broadcaster.clearCache();
            }
        }
    }
}
