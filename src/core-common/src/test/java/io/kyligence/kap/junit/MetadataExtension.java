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
package io.kyligence.kap.junit;

import java.io.File;
import java.lang.reflect.AnnotatedElement;
import java.lang.reflect.Field;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.QueryContext;
import org.apache.kylin.common.Singletons;
import org.apache.kylin.common.persistence.ResourceStore;
import org.junit.jupiter.api.extension.BeforeAllCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.InvocationInterceptor;
import org.junit.platform.commons.support.AnnotationSupport;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.TempMetadataBuilder;
import io.kyligence.kap.common.util.Unsafe;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.junit.annotation.MetadataInfo;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.val;

public class MetadataExtension implements BeforeEachCallback, BeforeAllCallback, InvocationInterceptor {

    private static final ExtensionContext.Namespace NAMESPACE = ExtensionContext.Namespace
            .create(MetadataExtension.class);
    private static final String METADATA_INFO_KEY = "MetadataInfo";

    @Override
    public void beforeAll(ExtensionContext context) throws Exception {
        readFromAnnotation(context.getElement()).ifPresent(x -> context.getStore(NAMESPACE).put(METADATA_INFO_KEY, x));
    }

    @SneakyThrows
    @Override
    public void beforeEach(ExtensionContext context) throws Exception {
        run(context, null);
    }

    private Optional<MetadataResource> readFromAnnotation(Optional<AnnotatedElement> element) {
        return AnnotationSupport.findAnnotation(element, MetadataInfo.class).map(MetadataResource::of);
    }

    private void run(ExtensionContext context, Invocation<Void> invocation) throws Throwable {
        readFromAnnotation(context.getElement())
                .orElse(context.getStore(NAMESPACE).get(METADATA_INFO_KEY, MetadataResource.class)).get();
        if (invocation != null) {
            invocation.proceed();
        }
    }

    @RequiredArgsConstructor
    private static class MetadataResource implements ExtensionContext.Store.CloseableResource {

        private TempMetadataBuilder metadataBuilder;
        private File tempMetadataDirectory;

        static MetadataResource of(MetadataInfo info) {
            val resource = new MetadataResource();
            resource.metadataBuilder = TempMetadataBuilder.createBuilder(Lists.newArrayList(info.overlay()));
            resource.metadataBuilder.setProject(info.project());
            resource.metadataBuilder.setOnlyProps(info.onlyProps());
            return resource;

        }

        public File get() {
            String tempMetadataDir = metadataBuilder.build();
            KylinConfig.setKylinConfigForLocalTest(tempMetadataDir);
            tempMetadataDirectory = new File(tempMetadataDir);
            try {
                Class.forName("org.h2.Driver");
            } catch (ClassNotFoundException e) {
                // ignore it
            }
            cleanSingletonInstances();

            val kylinHomePath = new File(getTestConfig().getMetadataUrl().toString()).getParentFile().getAbsolutePath();
            System.setProperty("KYLIN_HOME", kylinHomePath);
            val jobJar = io.kyligence.kap.common.util.FileUtils.findFile(
                    new File(kylinHomePath, "../../../assembly/target/").getAbsolutePath(), "kap-assembly(.?)\\.jar");
            getTestConfig().setProperty("kylin.engine.spark.job-jar", jobJar == null ? "" : jobJar.getAbsolutePath());
            getTestConfig().setProperty("kylin.query.security.acl-tcr-enabled", "false");
            return tempMetadataDirectory;
        }

        @Override
        public void close() throws Throwable {
            cleanSingletonInstances();
            clearTestConfig();
            System.clearProperty("KYLIN_HOME");
            QueryContext.reset();

            FileUtils.deleteQuietly(tempMetadataDirectory);
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

        static ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> getInstanceByProjectFromSingleton()
                throws Exception {
            Field instanceField = Singletons.class.getDeclaredField("instance");
            Unsafe.changeAccessibleObject(instanceField, true);
            Field field = Singletons.class.getDeclaredField("instancesByPrj");
            Unsafe.changeAccessibleObject(field, true);
            val result = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field
                    .get(instanceField.get(null));
            if (result == null) {
                field.set(instanceField.get(null), Maps.newConcurrentMap());
            }
            return (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field.get(instanceField.get(null));
        }

        static ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> getInstanceByProject() throws Exception {
            Field singletonField = getTestConfig().getClass().getDeclaredField("singletons");
            Unsafe.changeAccessibleObject(singletonField, true);
            Field field = Singletons.class.getDeclaredField("instancesByPrj");
            Unsafe.changeAccessibleObject(field, true);
            return (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field
                    .get(singletonField.get(getTestConfig()));
        }

        static ConcurrentHashMap<Class, Object> getInstancesFromSingleton() throws Exception {
            Field instanceField = Singletons.class.getDeclaredField("instance");
            Unsafe.changeAccessibleObject(instanceField, true);
            Field field = Singletons.class.getDeclaredField("instancesByPrj");
            Unsafe.changeAccessibleObject(field, true);
            val result = (ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>>) field
                    .get(instanceField.get(null));
            if (result == null) {
                field.set(instanceField.get(null), Maps.newConcurrentMap());
            }
            return (ConcurrentHashMap<Class, Object>) field.get(instanceField.get(null));
        }

        static ConcurrentHashMap<Class, Object> getInstances() throws Exception {
            Field singletonField = getTestConfig().getClass().getDeclaredField("singletons");
            Unsafe.changeAccessibleObject(singletonField, true);
            Field filed = Singletons.class.getDeclaredField("instances");
            Unsafe.changeAccessibleObject(filed, true);
            return (ConcurrentHashMap<Class, Object>) filed.get(singletonField.get(getTestConfig()));
        }

        static ConcurrentHashMap<Class, Object> getGlobalInstances() throws Exception {
            Field instanceFiled = Singletons.class.getDeclaredField("instance");
            Unsafe.changeAccessibleObject(instanceFiled, true);

            Singletons instanceSingle = (Singletons) instanceFiled.get(instanceFiled);

            Field instancesField = instanceSingle.getClass().getDeclaredField("instances");
            Unsafe.changeAccessibleObject(instancesField, true);

            return (ConcurrentHashMap<Class, Object>) instancesField.get(instanceSingle);
        }

        static KylinConfig getTestConfig() {
            return KylinConfig.getInstanceFromEnv();
        }

        static void clearTestConfig() {
            try {
                ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).close();
            } catch (Exception ignore) {
            }
            KylinConfig.destroyInstance();
        }

    }
}
