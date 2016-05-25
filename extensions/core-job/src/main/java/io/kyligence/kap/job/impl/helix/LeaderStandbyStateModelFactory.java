package io.kyligence.kap.job.impl.helix;

import org.apache.helix.api.StateTransitionHandlerFactory;
import org.apache.helix.api.TransitionHandler;
import org.apache.helix.api.id.PartitionId;
import org.apache.helix.api.id.ResourceId;
import org.apache.kylin.common.KylinConfig;

/**
 */
public class LeaderStandbyStateModelFactory extends StateTransitionHandlerFactory<TransitionHandler> {
    private final KylinConfig kylinConfig;

    public LeaderStandbyStateModelFactory(KylinConfig kylinConfig) {
        this.kylinConfig = kylinConfig;
    }

    @Override
    public TransitionHandler createStateTransitionHandler(PartitionId partitionId) {
        if (partitionId.getResourceId().equals(ResourceId.from(HelixClusterAdmin.RESOURCE_NAME_JOB_ENGINE))) {
            return JobEngineTransitionHandler.getInstance(kylinConfig);
        }

        return null;
    }

}
