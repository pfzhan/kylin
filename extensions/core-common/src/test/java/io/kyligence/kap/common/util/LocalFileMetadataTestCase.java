package io.kyligence.kap.common.util;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;

/**
 * Created by dongli on 4/7/16.
 */
public class LocalFileMetadataTestCase extends org.apache.kylin.common.util.LocalFileMetadataTestCase {
    public static String KAP_META_TEST_DATA = "../examples/test_case_data/localmeta";
    public static String KYLIN_META_TEST_DATA = "../../kylin/examples/test_case_data/localmeta";

    @Override
    public void createTestMetadata() {
        staticCreateTestMetadata();
    }

    public static void staticCreateTestMetadata() {
        KylinConfig.destoryInstance();

        String tempTestMetadataUrl = "../examples/test_metadata";
        try {
            FileUtils.deleteDirectory(new File(tempTestMetadataUrl));

            // KAP files will overwrite Kylin files
            FileUtils.copyDirectory(new File(KYLIN_META_TEST_DATA), new File(tempTestMetadataUrl));
            FileUtils.copyDirectory(new File(KAP_META_TEST_DATA), new File(tempTestMetadataUrl));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null)
            System.setProperty(KylinConfig.KYLIN_CONF, tempTestMetadataUrl);

        KylinConfig.getInstanceFromEnv().setMetadataUrl(tempTestMetadataUrl);
    }
}
