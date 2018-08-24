package io.kyligence.kap.smart;

import io.kyligence.kap.cube.model.NCubePlan;
import io.kyligence.kap.smart.cube.NCubeMaster;

public class NCubePlanShrinkProposer extends NAbstractProposer {

    public NCubePlanShrinkProposer(NSmartContext modelCtx) {
        super(modelCtx);
    }

    @Override
    void propose() {
        if (context.getModelContexts() == null)
            return;

        for (NSmartContext.NModelContext modelCtx : context.getModelContexts()) {
            if (modelCtx.getOrigModel() == null) {
                continue;
            }
            if (modelCtx.getOrigCubePlan() == null) {
                continue;
            }
            if (modelCtx.getTargetCubePlan() == null) {
                continue;
            }
            
            NCubeMaster cubeMaster = new NCubeMaster(modelCtx);
            NCubePlan cubePlan = modelCtx.getTargetCubePlan();
            cubePlan = cubeMaster.reduceCuboids(cubePlan);
            modelCtx.setTargetCubePlan(cubePlan);
        }
    }
}
