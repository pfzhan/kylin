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

import java.io.File;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.LocalFileMetadataTestCase;

public class NLocalFileMetadataTestCase extends LocalFileMetadataTestCase {
    public static File tempMetadataDirectory = null;

    static {
        System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");
    }

    @Override
    public void createTestMetadata(String ... overlay) {
        staticCreateTestMetadata(overlay);
    }

    public void createTestMetadata() {
        staticCreateTestMetadata();
    }

    @Override
    public void cleanupTestMetadata() {
        try {
            Class<?> clz = Class.forName("org.apache.kylin.metadata.cachesync.Broadcaster");
            Object broadcaster = clz.getMethod("getInstance", KylinConfig.class).invoke(null, getTestConfig());
            clz.getMethod("notifyClearAll").invoke(broadcaster);
        } catch (Exception e) {
            // ignore
        }

        super.cleanupTestMetadata();
    }

    public static void staticCreateTestMetadata(String... overlay) {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata(false, overlay);
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        tempMetadataDirectory = new File(tempMetadataDir);
    }

    public static void staticCreateTestMetadata() {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        tempMetadataDirectory = new File(tempMetadataDir);
    }

    public static KylinConfig getTestConfig() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        return config;
    }
}
