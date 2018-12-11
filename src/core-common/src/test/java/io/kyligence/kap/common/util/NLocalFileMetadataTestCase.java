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
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NLocalFileMetadataTestCase extends AbstractKylinTestCase {

    private static final Logger logger = LoggerFactory.getLogger(NLocalFileMetadataTestCase.class);
    private static final String LOCALMETA_TEMP_DATA = "../examples/test_metadata/";
    protected static File tempMetadataDirectory = null;

    static {
        if (new File("../../build/conf/kylin-tools-log4j.properties").exists()) {
            System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");
        } else {
            System.setProperty("log4j.configuration", "file:../../../build/conf/kylin-tools-log4j.properties");
        }
    }

    @Override
    public void createTestMetadata(String... overlay) {
        staticCreateTestMetadata(overlay);
    }

    public void createTestMetadata() {
        staticCreateTestMetadata();
    }

    @Override
    public void cleanupTestMetadata() {
        staticCleanupTestMetadata();
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

    public static void staticCleanupTestMetadata() {
        File directory = new File(LOCALMETA_TEMP_DATA);
        deleteDirectoryWithErrMsg(directory);

        clearTestConfig();
    }

    public static void deleteDirectoryWithErrMsg(File dir) {
        int retry = 3;
        String msg = null;
        IOException ioe = null;

        while (retry > 0) {
            msg = null;
            ioe = null;
            boolean done = true;
            try {
                FileUtils.deleteDirectory(dir);
            } catch (IOException e) {
                Collection<File> remaining = FileUtils.listFiles(dir, null, true);
                if (!remaining.isEmpty()) {
                    done = false;
                    ioe = e;
                    msg = "Cannot delete directory " + dir + ", remaining: " + remaining;
                    logger.error(msg, e);
                }
            }
            if (done) {
                break;
            }

            try {
                Thread.sleep(300);
            } catch (InterruptedException e) {
                logger.warn("Interrupted!", e);
                Thread.currentThread().interrupt();
            }
            retry--;
        }

        if (ioe != null)
            throw new IllegalStateException(msg, ioe);
    }

    protected String getLocalWorkingDirectory() {
        String dir = KylinConfig.getInstanceFromEnv().getHdfsWorkingDirectory();
        if (dir.startsWith("file://"))
            dir = dir.substring("file://".length());
        try {
            return new File(dir).getCanonicalPath();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    protected ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
    }

}
