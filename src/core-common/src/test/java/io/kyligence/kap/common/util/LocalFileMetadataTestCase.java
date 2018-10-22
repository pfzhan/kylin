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

package io.kyligence.kap.common.util;

import org.apache.kylin.common.KylinConfig;

public class LocalFileMetadataTestCase extends org.apache.kylin.common.util.LocalFileMetadataTestCase {

    static {
        System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");
    }

    public void createTestMetadata() {
        staticCreateTestMetadata();
    }

    public static void staticCreateTestMetadata() {

        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata();

        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
    }

    @Override
    public void createTestMetadata(String... overlayMetadataDirs) {
        staticCreateTestMetadata(overlayMetadataDirs);
    }

    public static void staticCreateTestMetadata(String... overlayMetadataDirs) {
        staticCreateTestMetadata(true, overlayMetadataDirs);
    }

    public static void staticCreateTestMetadata(boolean useDefaultMetadata, String... overlayMetadataDirs) {
        KylinConfig.destroyInstance();

        String tempMetadataDir = LocalTempMetadata.prepareLocalTempMetadata(useDefaultMetadata,
                new OverlayMetaHook(overlayMetadataDirs));

        if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null)
            System.setProperty(KylinConfig.KYLIN_CONF, tempMetadataDir);

        KylinConfig config = KylinConfig.getInstanceFromEnv();
        config.setMetadataUrl(tempMetadataDir);
        config.setProperty("kylin.env.hdfs-working-dir", "file:///tmp/kylin");
    }
}
