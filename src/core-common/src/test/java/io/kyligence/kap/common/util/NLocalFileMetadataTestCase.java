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
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.AbstractTestCase;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.Assert;
import org.junit.Before;
import org.mockito.Mockito;

import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import lombok.val;

@Deprecated
public class NLocalFileMetadataTestCase extends AbstractTestCase {

    protected static File tempMetadataDirectory = null;
    Map<Object, Object> originManager = Maps.newHashMap();

    @Before
    public void setNeedCheckCC() {
        overwriteSystemProp("needCheckCC", "true");
    }

    public static File getTempMetadataDirectory() {
        return tempMetadataDirectory;
    }

    public static ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> getInstanceByProjectFromSingleton()
            throws Exception {
        Field instanceField = Singletons.class.getDeclaredField("instance");
        Unsafe.changeAccessibleObject(instanceField, true);
        Field field = Singletons.class.getDeclaredField("instancesByPrj");
        Unsafe.changeAccessibleObject(field, true);
        val result = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
        if (result == null) {
            field.set(instanceField.get(null), Maps.newConcurrentMap());
        }
        return (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
    }

    public static ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> getInstanceByProject() throws Exception {
        Field singletonField = getTestConfig().getClass().getDeclaredField("singletons");
        Unsafe.changeAccessibleObject(singletonField, true);
        Field field = Singletons.class.getDeclaredField("instancesByPrj");
        Unsafe.changeAccessibleObject(field, true);
        return (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field
                .get(singletonField.get(getTestConfig()));
    }

    public static ConcurrentHashMap<Class, Object> getInstancesFromSingleton() throws Exception {
        Field instanceField = Singletons.class.getDeclaredField("instance");
        Unsafe.changeAccessibleObject(instanceField, true);
        Field field = Singletons.class.getDeclaredField("instancesByPrj");
        Unsafe.changeAccessibleObject(field, true);
        val result = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
        if (result == null) {
            field.set(instanceField.get(null), Maps.newConcurrentMap());
        }
        return (ConcurrentHashMap<Class, Object>) field.get(instanceField.get(null));
    }

    public static ConcurrentHashMap<Class, Object> getInstances() throws Exception {
        Field singletonField = getTestConfig().getClass().getDeclaredField("singletons");
        Unsafe.changeAccessibleObject(singletonField, true);
        Field filed = Singletons.class.getDeclaredField("instances");
        Unsafe.changeAccessibleObject(filed, true);
        return (ConcurrentHashMap<Class, Object>) filed.get(singletonField.get(getTestConfig()));
    }

    public static ConcurrentHashMap<Class, Object> getGlobalInstances() throws Exception {
        Field instanceFiled = Singletons.class.getDeclaredField("instance");
        Unsafe.changeAccessibleObject(instanceFiled, true);

        Singletons instanceSingle = (Singletons) instanceFiled.get(instanceFiled);

        Field instancesField = instanceSingle.getClass().getDeclaredField("instances");
        Unsafe.changeAccessibleObject(instancesField, true);

        return (ConcurrentHashMap<Class, Object>) instancesField.get(instanceSingle);
    }

    public <T> T spyManagerByProject(T t, Class<T> tClass,
            ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> cache, String project) {
        T manager = Mockito.spy(t);
        originManager.put(manager, t);
        ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = cache;
        if (managersByPrjCache.get(tClass) == null) {
            managersByPrjCache.put(tClass, new ConcurrentHashMap<>());
        }
        managersByPrjCache.get(tClass).put(project, manager);
        return manager;
    }

    public <T> T spyManagerByProject(T t, Class<T> tClass, String project) throws Exception {
        return spyManagerByProject(t, tClass, getInstanceByProject(), project);
    }

    public <T> T spyManager(T t, Class<T> tClass) throws Exception {
        T manager = Mockito.spy(t);
        originManager.put(manager, t);
        ConcurrentHashMap<Class, Object> managersCache = getInstances();
        managersCache.put(tClass, manager);
        return manager;
    }

    public <T, M> T spy(M m, Function<M, T> functionM, Function<T, T> functionT) {
        return functionM.apply(Mockito.doAnswer(answer -> {
            T t = functionM.apply((M) originManager.get(m));
            return functionT.apply(t);
        }).when(m));
    }

    public void createTestMetadata(String... overlay) {
        staticCreateTestMetadata(overlay);
        val kylinHomePath = new File(getTestConfig().getMetadataUrl().toString()).getParentFile().getAbsolutePath();
        overwriteSystemProp("KYLIN_HOME", kylinHomePath);
        val jobJar = io.kyligence.kap.common.util.FileUtils.findFile(
                new File(kylinHomePath, "../../../assembly/target/").getAbsolutePath(), "kap-assembly(.?)\\.jar");
        getTestConfig().setProperty("kylin.engine.spark.job-jar", jobJar == null ? "" : jobJar.getAbsolutePath());
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        getTestConfig().setProperty("kylin.streaming.enabled", "true");
    }

    public void cleanupTestMetadata() {
        staticCleanupTestMetadata();
    }

    public static void staticCreateTestMetadata(String... overlay) {
        String tempMetadataDir = TempMetadataBuilder.prepareLocalTempMetadata(Lists.newArrayList(overlay));
        KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
        tempMetadataDirectory = new File(tempMetadataDir);
        getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
        try {
            Class.forName("org.h2.Driver");
        } catch (ClassNotFoundException e) {
            // ignore it
        }
        cleanSingletonInstances();
    }

    private static void cleanSingletonInstances() {
        try {
            getInstances().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getGlobalInstances().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstancesFromSingleton().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstanceByProjectFromSingleton().clear();
        } catch (Exception e) {
            //ignore in it
        }

        try {
            getInstanceByProject().clear();
        } catch (Exception e) {
            //ignore in it
        }
    }

    private static void clearTestConfig() {
        try {
            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).close();
        } catch (Exception ignore) {
        }
        KylinConfig.destroyInstance();
    }

    public static KylinConfig getTestConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    public static void staticCleanupTestMetadata() {
        cleanSingletonInstances();
        clearTestConfig();
        QueryContext.reset();

        File directory = new File(TempMetadataBuilder.TEMP_TEST_METADATA);
        FileUtils.deleteQuietly(directory);

    }

    public static String getLocalWorkingDirectory() {
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

    protected Map<Integer, Long> createKafkaPartitionOffset(int partition, Long offset) {
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        map.put(partition, offset);
        return map;
    }

    protected Map<Integer, Long> createKafkaPartitionsOffset(int partitionNumbers, Long offset) {
        Map<Integer, Long> map = new HashMap<Integer, Long>();
        for (int i = 0; i < partitionNumbers; i++) {
            map.put(i, offset);
        }
        return map;
    }

    public void assertKylinExeption(UserFunction f, String msg) {
        try {
            f.process();
            Assert.fail();
        } catch (Exception e) {
            Assert.assertTrue(e instanceof KylinException);
            if (StringUtils.isNotEmpty(msg)) {
                Assert.assertTrue(e.getMessage().contains(msg));
            }
        }
    }

    public interface UserFunction {
        void process() throws Exception;
    }
}
