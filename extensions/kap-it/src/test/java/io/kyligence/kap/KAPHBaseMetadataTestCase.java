
package io.kyligence.kap;

import java.io.File;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.apache.kylin.common.util.ClassUtil;

public class KAPHBaseMetadataTestCase extends AbstractKylinTestCase {

    public static String SANDBOX_TEST_DATA = "../examples/test_case_data/sandbox";

    static {
        try {
            ClassUtil.addClasspath(new File(SANDBOX_TEST_DATA).getAbsolutePath());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void createTestMetadata() throws Exception {
        staticCreateTestMetadata();
    }

    @Override
    public void cleanupTestMetadata() {
        staticCleanupTestMetadata();
    }

    public static void staticCreateTestMetadata() throws Exception {
        staticCreateTestMetadata(SANDBOX_TEST_DATA);
    }

    public static void staticCreateTestMetadata(String kylinConfigFolder) {

        KylinConfig.destroyInstance();

        if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null)
            System.setProperty(KylinConfig.KYLIN_CONF, kylinConfigFolder);

    }

}
