package io.kyligence.kap.provision;

import io.kyligence.kap.KAPDeployUtil;
import org.apache.kylin.job.DeployUtil;

public class ProvisionWithoutBuild extends org.apache.kylin.provision.BuildCubeWithEngine{
    public static void main(String[] args) throws Exception {
        beforeClass();
        KAPDeployUtil.deployMetadata();
        System.out.println("A");
        DeployUtil.prepareTestDataForNormalCubes("test_kylin_cube_with_slr_empty");
    }
}
