package io.kyligence.kap.provision;

import io.kyligence.kap.KAPDeployUtil;
import org.apache.kylin.job.DeployUtil;

public class ProvisionWithoutBuild extends org.apache.kylin.provision.BuildCubeWithEngine {
    public static void main(String[] args) throws Exception {
        beforeClass();
        KAPDeployUtil.deployMetadata();
        DeployUtil.prepareTestDataForNormalCubes("ci_left_join_model");
    }
}
