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
import java.lang.reflect.Field;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.AbstractKylinTestCase;
import org.junit.After;

import com.google.common.collect.Maps;

import lombok.val;

public class NLocalFileMetadataTestCase extends AbstractKylinTestCase {

    public static final String H2_METADATA_URL_PATTERN = "%s@jdbc,driverClassName=org.h2.Driver,url=jdbc:h2:mem:db_default;DB_CLOSE_DELAY=-1,username=sa,password=";
    private static final String LOCALMETA_TEMP_DATA = "../examples/test_metadata/";
    protected static File tempMetadataDirectory = null;

    public static ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> getInstanceByProjectFromSingleton()
            throws Exception {
        Field instanceField = Singletons.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        Field field = Singletons.class.getDeclaredField("instancesByPrj");
        field.setAccessible(true);
        val result = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
        if (result == null) {
            field.set(instanceField.get(null), Maps.newConcurrentMap());
        }
        return (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
    }

    public static ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> getInstanceByProject() throws Exception {
        Field singletonField = getTestConfig().getClass().getDeclaredField("singletons");
        singletonField.setAccessible(true);
        Field field = Singletons.class.getDeclaredField("instancesByPrj");
        field.setAccessible(true);
        return (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field
                .get(singletonField.get(getTestConfig()));
    }

    public static ConcurrentHashMap<Class, Object> getInstancesFromSingleton() throws Exception {
        Field instanceField = Singletons.class.getDeclaredField("instance");
        instanceField.setAccessible(true);
        Field field = Singletons.class.getDeclaredField("instances");
        field.setAccessible(true);
        val result = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
        if (result == null) {
            field.set(instanceField.get(null), Maps.newConcurrentMap());
        }
        return (ConcurrentHashMap<Class, Object>) field.get(instanceField.get(null));
    }

    public static ConcurrentHashMap<Class, Object> getInstances() throws Exception {
        Field singletonField = getTestConfig().getClass().getDeclaredField("singletons");
        singletonField.setAccessible(true);
        Field filed = Singletons.class.getDeclaredField("instances");
        filed.setAccessible(true);
        return (ConcurrentHashMap<Class, Object>) filed.get(singletonField.get(getTestConfig()));
    }

    @Override
    public void createTestMetadata(String... overlay) {
        staticCreateTestMetadata(overlay);
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
    }

    public void createTestMetadata() {
        staticCreateTestMetadata();
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
    }

    @Override
    public void cleanupTestMetadata() {
        staticCleanupTestMetadata();
    }

    @After
    public void restoreSystemProps() {
        restoreAllSystemProp();
    }

    public static void staticCreateTestMetadata(String... overlay) {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata(false, overlay);
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        tempMetadataDirectory = new File(tempMetadataDir);
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
    }

    public static void staticCreateTestMetadata() {
        String tempMetadataDir = TempMetadataBuilder.prepareNLocalTempMetadata();
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        tempMetadataDirectory = new File(tempMetadataDir);
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            // ignore it
        }
    }

    public static KylinConfig getTestConfig() {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        return config;
    }

    public static void staticCleanupTestMetadata() {
        File directory = new File(LOCALMETA_TEMP_DATA);
        FileUtils.deleteQuietly(directory);

        clearTestConfig();
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
