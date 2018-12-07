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


/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *     http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
*/

package org.apache.kylin.common.util;

import java.io.File;
import java.io.IOException;
import java.util.Collection;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalFileMetadataTestCase extends AbstractKylinTestCase {
    private static final Logger logger = LoggerFactory.getLogger(LocalFileMetadataTestCase.class);

    public static final String LOCALMETA_TEST_DATA = "../examples/test_case_data/localmeta_n";
    public static final String LOCALMETA_TEMP_DATA = "../examples/test_metadata/";

    @Override
    public void createTestMetadata(String... overlayMetadataDirs) {
        staticCreateTestMetadata(true, new OverlayMetaHook(overlayMetadataDirs));
    }

    public static void staticCreateTestMetadata(String... overlayMetadataDirs) {
        staticCreateTestMetadata(true, new OverlayMetaHook(overlayMetadataDirs));
    }

    public static void staticCreateTestMetadata(boolean useTestMeta, MetadataTestCaseHook hook) {
        try {
            KylinConfig.destroyInstance();

            FileUtils.deleteDirectory(new File(LOCALMETA_TEMP_DATA));
            if (useTestMeta) {
                FileUtils.copyDirectory(new File(LOCALMETA_TEST_DATA), new File(LOCALMETA_TEMP_DATA));
            }

            if (System.getProperty(KylinConfig.KYLIN_CONF) == null && System.getenv(KylinConfig.KYLIN_CONF) == null) {
                System.setProperty(KylinConfig.KYLIN_CONF, LOCALMETA_TEMP_DATA);
            }

            if (hook != null) {
                hook.hook();
            }
            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.setMetadataUrl(LOCALMETA_TEMP_DATA);
            config.setProperty("kylin.env.hdfs-working-dir", "file:///tmp/kylin");
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
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
                    msg = "Can't delete directory " + dir + ", remaining: " + remaining;
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

    @Override
    public void cleanupTestMetadata() {
        staticCleanupTestMetadata();
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

    public interface MetadataTestCaseHook {
        void hook() throws IOException;
    }

    public static class OverlayMetaHook implements MetadataTestCaseHook {
        private String[] overlayMetadataDirs;

        public OverlayMetaHook(String... overlayMetadataDirs) {
            this.overlayMetadataDirs = overlayMetadataDirs;
        }

        @Override
        public void hook() throws IOException {
            //some test cases may require additional metadata entries besides standard test metadata in test_case_data/localmeta
            for (String overlay : overlayMetadataDirs) {
                FileUtils.copyDirectory(new File(overlay), new File(LOCALMETA_TEMP_DATA));
            }
        }
    }

    public static class ExcludeMetaHook implements MetadataTestCaseHook {
        private String[] excludeMetadataDirs;

        public ExcludeMetaHook(String... excludeMetadataDirs) {
            this.excludeMetadataDirs = excludeMetadataDirs;
        }

        @Override
        public void hook() throws IOException {
            //some test cases may want exclude metadata entries besides standard test metadata in test_case_data/localmeta
            for (String exclude : excludeMetadataDirs) {
                FileUtils.deleteQuietly(new File(exclude));
            }
        }
    }
}
