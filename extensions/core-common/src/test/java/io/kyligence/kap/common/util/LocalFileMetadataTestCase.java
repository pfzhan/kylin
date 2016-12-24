package io.kyligence.kap.common.util;

import org.apache.kylin.common.KylinConfig;

/**
 * Created by dongli on 4/7/16.
 */
public class LocalFileMetadataTestCase extends org.apache.kylin.common.util.LocalFileMetadataTestCase {

    @Override
    public void createTestMetadata() {
        staticCreateTestMetadata();
    }

    public static void staticCreateTestMetadata() {
        KylinConfig.destroyInstance();

        String tempMetadataDir = LocalTempMetadata.prepareLocalTempMetadata();

        if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null)
            System.setProperty(KylinConfig.KYLIN_CONF, tempMetadataDir);

        KylinConfig.getInstanceFromEnv().setMetadataUrl(tempMetadataDir);
    }

}
