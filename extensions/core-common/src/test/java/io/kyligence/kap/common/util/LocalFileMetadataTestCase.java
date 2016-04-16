package io.kyligence.kap.common.util;

/**
 * Created by dongli on 4/7/16.
 */
public class LocalFileMetadataTestCase extends org.apache.kylin.common.util.LocalFileMetadataTestCase {
    public static String LOCALMETA_TEST_DATA = "../examples/test_case_data/localmeta";

    @Override
    public void createTestMetadata() {
        staticCreateTestMetadata(LOCALMETA_TEST_DATA);
    }
}
