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

package org.apache.kylin.common;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.lang.reflect.Method;
import java.net.URL;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.common.util.ClassUtil;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.OrderedProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 */
public class KylinConfig extends KylinConfigBase {
    private static final long serialVersionUID = 1L;
    private static final Logger logger = LoggerFactory.getLogger(KylinConfig.class);

    /**
     * Kylin properties file name
     */
    public static final String KYLIN_CONF_PROPERTIES_FILE = "kylin.properties";
    public static final String KYLIN_CONF = "KYLIN_CONF";

    // static cached instances
    private static KylinConfig SYS_ENV_INSTANCE = null;

    // thread-local instances, will override SYS_ENV_INSTANCE
    private static transient ThreadLocal<KylinConfig> THREAD_ENV_INSTANCE = new ThreadLocal<>();

    static {
        /*
         * Make Calcite to work with Unicode.
         *
         * Sets default char set for string literals in SQL and row types of
         * RelNode. This is more a label used to compare row type equality. For
         * both SQL string and row record, they are passed to Calcite in String
         * object and does not require additional codec.
         *
         * Ref SaffronProperties.defaultCharset
         * Ref SqlUtil.translateCharacterSetName()
         * Ref NlsString constructor()
         */
        // copied from org.apache.calcite.util.ConversionUtil.NATIVE_UTF16_CHARSET_NAME
        String NATIVE_UTF16_CHARSET_NAME = (ByteOrder.nativeOrder() == ByteOrder.BIG_ENDIAN) ? "UTF-16BE" : "UTF-16LE";
        System.setProperty("saffron.default.charset", NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.nationalcharset", NATIVE_UTF16_CHARSET_NAME);
        System.setProperty("saffron.default.collation.name", NATIVE_UTF16_CHARSET_NAME + "$en_US");
    }

    public static KylinConfig getInstanceFromEnv() {
        synchronized (KylinConfig.class) {
            KylinConfig config = THREAD_ENV_INSTANCE.get();
            if (config != null) {
                logger.info("Using thread local KylinConfig");
                return config;
            }

            if (SYS_ENV_INSTANCE == null) {
                try {
                    config = new KylinConfig();
                    config.reloadKylinConfig(buildSiteProperties());

                    logger.info("Initialized a new KylinConfig from getInstanceFromEnv : "
                            + System.identityHashCode(config));
                    SYS_ENV_INSTANCE = config;
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Failed to find KylinConfig ", e);
                }
            }
            return SYS_ENV_INSTANCE;
        }
    }

    // Only used in test cases!!!
    public static void setKylinConfigForLocalTest(String localMetaDir) {
        synchronized (KylinConfig.class) {
            if (new File(localMetaDir, "kylin.properties").exists() == false)
                throw new IllegalArgumentException(localMetaDir + " is not a valid local meta dir");

            destroyInstance();
            logger.info("Setting KylinConfig to " + localMetaDir);

            System.setProperty(KylinConfig.KYLIN_CONF, localMetaDir);

            KylinConfig config = KylinConfig.getInstanceFromEnv();
            config.setMetadataUrl(localMetaDir + "/metadata");

            // make sure a local working directory
            File workingDir = new File(localMetaDir, "working-dir");
            workingDir.mkdirs();
            String path = workingDir.getAbsolutePath();
            if (!path.startsWith("/"))
                path = "/" + path;
            if (!path.endsWith("/"))
                path = path + "/";
            path = path.replace("\\", "/");
            config.setProperty("kylin.env.hdfs-working-dir", "file:" + path);
        }
    }

    // Only used in test cases!!!
    public static void destroyInstance() {
        synchronized (KylinConfig.class) {
            logger.info("Destroy KylinConfig");
            dumpStackTrace();
            SYS_ENV_INSTANCE = null;
            THREAD_ENV_INSTANCE = new ThreadLocal<>();
        }
    }

    private static void dumpStackTrace() {

        //uncomment below to start debugging

        //        Thread t = Thread.currentThread();
        //        int maxStackTraceDepth = 20;
        //        int current = 0;
        //
        //        StackTraceElement[] stackTrace = t.getStackTrace();
        //        StringBuilder buf = new StringBuilder("This is not a exception, just for diagnose purpose:");
        //        buf.append("\n");
        //        for (StackTraceElement e : stackTrace) {
        //            if (++current > maxStackTraceDepth) {
        //                break;
        //            }
        //            buf.append("\t").append("at ").append(e.toString()).append("\n");
        //        }
        //        logger.info(buf.toString());
    }

    public enum UriType {
        PROPERTIES_FILE, REST_ADDR, LOCAL_FOLDER, HDFS_FILE
    }

    public static UriType decideUriType(String metaUri) {

        try {
            File file = new File(metaUri);
            if (file.exists() || metaUri.contains("/")) {
                if (file.exists() == false) {
                    file.mkdirs();
                }
                if (file.isDirectory()) {
                    return UriType.LOCAL_FOLDER;
                } else if (file.isFile()) {
                    if (file.getName().equalsIgnoreCase(KYLIN_CONF_PROPERTIES_FILE)) {
                        return UriType.PROPERTIES_FILE;
                    } else {
                        throw new IllegalStateException(
                                "Metadata uri : " + metaUri + " is a local file but not kylin.properties");
                    }
                } else {
                    throw new IllegalStateException(
                            "Metadata uri : " + metaUri + " looks like a file but it's neither a file nor a directory");
                }
            } else {
                if (RestClient.matchFullRestPattern(metaUri))
                    return UriType.REST_ADDR;
                else
                    throw new IllegalStateException("Metadata uri : " + metaUri + " is not a valid REST URI address");
            }
        } catch (Exception e) {
            throw new IllegalStateException("Metadata uri : " + metaUri + " is not recognized", e);
        }
    }

    public static KylinConfig createInstanceFromUri(String uri) {
        /**
         * --hbase:
         *
         * 1. PROPERTIES_FILE: path to kylin.properties
         * 2. REST_ADDR: rest service resource, format: user:password@host:port
         *
         * --local:
         *
         * 1.  LOCAL_FOLDER: path to resource folder
         */
        UriType uriType = decideUriType(uri);

        if (uriType == UriType.LOCAL_FOLDER) {
            KylinConfig config = new KylinConfig();
            config.setMetadataUrl(uri);
            return config;
        } else if (uriType == UriType.PROPERTIES_FILE) {
            KylinConfig config;
            try {
                config = new KylinConfig();
                InputStream is = new FileInputStream(uri);
                Properties prop = streamToProps(is);
                config.reloadKylinConfig(prop);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return config;
        } else {// rest_addr
            try {
                KylinConfig config = new KylinConfig();
                RestClient client = new RestClient(uri);
                String propertyText = client.getKylinProperties();
                InputStream is = IOUtils.toInputStream(propertyText, Charset.defaultCharset());
                Properties prop = streamToProps(is);
                config.reloadKylinConfig(prop);
                return config;
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static Properties streamToProps(InputStream is) throws IOException {
        Properties prop = new Properties();
        prop.load(is);
        IOUtils.closeQuietly(is);
        return prop;
    }

    public static void setKylinConfigInEnvIfMissing(Properties prop) {
        synchronized (KylinConfig.class) {
            if (SYS_ENV_INSTANCE == null) {
                try {
                    KylinConfig config = new KylinConfig();
                    config.reloadKylinConfig(prop);
                    logger.info("Resetting SYS_ENV_INSTANCE by a input stream: " + System.identityHashCode(config));
                    SYS_ENV_INSTANCE = config;
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("Failed to find KylinConfig ", e);
                }
            }
        }
    }

    public static void setKylinConfigInEnvIfMissing(String propsInStr) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(propsInStr));
        setKylinConfigInEnvIfMissing(props);
    }

    public static void setKylinConfigThreadLocal(KylinConfig config) {
        THREAD_ENV_INSTANCE.set(config);
    }

    public static void removeKylinConfigThreadLocal() {
        THREAD_ENV_INSTANCE.remove();
    }

    public static KylinConfig createKylinConfig(String propsInStr) throws IOException {
        Properties props = new Properties();
        props.load(new StringReader(propsInStr));
        return createKylinConfig(props);
    }

    public static KylinConfig createKylinConfig(KylinConfig another) {
        return createKylinConfig(another.getRawAllProperties());
    }

    public static KylinConfig createKylinConfig(Properties prop) {
        KylinConfig kylinConfig = new KylinConfig();
        kylinConfig.reloadKylinConfig(prop);
        return kylinConfig;
    }

    public static File getKylinConfDir() {
        return getSitePropertiesFile().getParentFile();
    }

    // should be private; package visible for test only
    static File getSitePropertiesFile() {
        String kylinConfHome = System.getProperty(KYLIN_CONF);
        if (!StringUtils.isEmpty(kylinConfHome)) {
            logger.info("Use KYLIN_CONF=" + kylinConfHome);
            return existFile(kylinConfHome);
        }

        logger.debug("KYLIN_CONF property was not set, will seek KYLIN_HOME env variable");

        String kylinHome = getKylinHome();
        if (StringUtils.isEmpty(kylinHome))
            throw new KylinConfigCannotInitException("Didn't find KYLIN_CONF or KYLIN_HOME, please set one of them");

        String path = kylinHome + File.separator + "conf";
        return existFile(path);
    }

    /**
     * Return a File only if it exists
     */
    private static File existFile(String path) {
        if (path == null) {
            return null;
        }

        return new File(path, KYLIN_CONF_PROPERTIES_FILE);
    }

    // build kylin properties from site deployment, a.k.a KYLIN_HOME/conf/kylin.properties
    private static Properties buildSiteProperties() {
        Properties conf = new Properties();

        OrderedProperties orderedProperties = buildSiteOrderedProps();
        for (Map.Entry<String, String> each : orderedProperties.entrySet()) {
            conf.put(each.getKey(), each.getValue());
        }

        return conf;
    }

    // build kylin properties from site deployment, a.k.a KYLIN_HOME/conf/kylin.properties
    private static OrderedProperties buildSiteOrderedProps() {

        try {
            // 1. load default configurations from classpath. 
            // we have a kylin-defaults.properties in kylin/core-common/src/main/resources 
            URL resource = Thread.currentThread().getContextClassLoader().getResource("kylin-defaults.properties");
            Preconditions.checkNotNull(resource);
            logger.info("Loading kylin-defaults.properties from {}", resource.getPath());
            OrderedProperties orderedProperties = new OrderedProperties();
            loadPropertiesFromInputStream(resource.openStream(), orderedProperties);

            for (int i = 0; i < 10; i++) {
                String fileName = "kylin-defaults" + (i) + ".properties";
                URL additionalResource = Thread.currentThread().getContextClassLoader().getResource(fileName);
                if (additionalResource != null) {
                    logger.info("Loading {} from {} ", fileName, additionalResource.getPath());
                    loadPropertiesFromInputStream(additionalResource.openStream(), orderedProperties);
                }
            }

            // 2. load site conf, to keep backward compatibility it's still named kylin.properties
            // actually it's better to be named kylin-site.properties
            File propFile = getSitePropertiesFile();
            if (propFile == null || !propFile.exists()) {
                logger.error("fail to locate " + KYLIN_CONF_PROPERTIES_FILE);
                throw new RuntimeException("fail to locate " + KYLIN_CONF_PROPERTIES_FILE);
            }
            loadPropertiesFromInputStream(new FileInputStream(propFile), orderedProperties);

            // 3. still support kylin.properties.override as secondary override
            // not suggest to use it anymore
            File propOverrideFile = new File(propFile.getParentFile(), propFile.getName() + ".override");
            if (propOverrideFile.exists()) {
                loadPropertiesFromInputStream(new FileInputStream(propOverrideFile), orderedProperties);
            }
            return orderedProperties;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public static KylinConfig loadKylinPropsAndMetadata() throws IOException {
        File metaDir = new File("meta");
        if (!metaDir.getAbsolutePath().equals(System.getProperty(KylinConfig.KYLIN_CONF))) {
            System.setProperty(KylinConfig.KYLIN_CONF, metaDir.getAbsolutePath());
            logger.info("The absolute path for meta dir is " + metaDir.getAbsolutePath());
            KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
            Map<String, String> paramsMap = new HashMap<>();
            paramsMap.put("path", metaDir.getAbsolutePath());
            StorageURL storageURL = new StorageURL(kylinConfig.getMetadataUrl().getIdentifier(), "ifile", paramsMap);
            kylinConfig.setMetadataUrl(storageURL.toString());
            return kylinConfig;
        } else {
            return KylinConfig.getInstanceFromEnv();
        }
    }


    public static KylinConfig loadKylinConfigFromHdfsIfNeeded(String uri) {
        KylinConfig config;
        try {
            config = KylinConfig.getInstanceFromEnv();
            //In some case, the KylinConfig from env is not the expected one.
            String uriFromEnv = config.getMetadataUrl().toString();
            if (!uriFromEnv.equalsIgnoreCase(uri))
                throw new KylinConfigCannotInitException("Not the expected KylinConfig");
        } catch (KylinConfigCannotInitException e) {
            logger.warn("No available KylinConfig in environment, fetch it from URI.");
            config = loadKylinConfigFromHdfs(uri);
            KylinConfig.setKylinConfigThreadLocal(config);
        }
        return config;
    }

    public static KylinConfig loadKylinConfigFromHdfs(String uri) {
        if (uri == null)
            throw new IllegalArgumentException("StorageUrl should not be null");
        if (!uri.contains("@hdfs"))
            throw new IllegalArgumentException("StorageUrl should like @hdfs schema");
        logger.info("Ready to load KylinConfig from uri: {}", uri);
        StorageURL url = StorageURL.valueOf(uri);
        String metaDir = url.getParameter("path") + "/" + KylinConfig.KYLIN_CONF_PROPERTIES_FILE;
        try {
            FileSystem fs = HadoopUtil.getFileSystem(metaDir);
            InputStream is = fs.open(new Path(metaDir));
            Properties prop = KylinConfig.streamToProps(is);
            KylinConfig config = KylinConfig.createKylinConfig(prop);
            KylinConfig.setKylinConfigThreadLocal(config);
            return config;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * will close the passed in inputstream
     */
    private static void loadPropertiesFromInputStream(InputStream inputStream, OrderedProperties properties) {
        Preconditions.checkNotNull(properties);
        BufferedReader confReader = null;
        try {
            confReader = new BufferedReader(new InputStreamReader(inputStream, "UTF-8"));
            OrderedProperties temp = new OrderedProperties();
            temp.load(confReader);
            temp = BCC.check(temp);

            properties.putAll(temp);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            IOUtils.closeQuietly(confReader);
        }
    }

    public static void setSandboxEnvIfPossible() {
        File dir1 = new File("../examples/test_case_data/sandbox");
        File dir2 = new File("../../kylin/examples/test_case_data/sandbox");

        if (dir1.exists()) {
            logger.info("Setting sandbox env, KYLIN_CONF=" + dir1.getAbsolutePath());
            ClassUtil.addClasspath(dir1.getAbsolutePath());
            System.setProperty(KylinConfig.KYLIN_CONF, dir1.getAbsolutePath());
        } else if (dir2.exists()) {
            logger.info("Setting sandbox env, KYLIN_CONF=" + dir2.getAbsolutePath());
            ClassUtil.addClasspath(dir2.getAbsolutePath());
            System.setProperty(KylinConfig.KYLIN_CONF, dir2.getAbsolutePath());
        }
    }

    // ============================================================================

    transient ConcurrentHashMap<Class, Object> managersCache = null;
    transient ConcurrentHashMap<Class, ConcurrentHashMap<String, Object>> managersByPrjCache = null;

    private KylinConfig() {
        super();
    }

    protected KylinConfig(Properties props, boolean force) {
        super(props, force);
    }

    public <T> T getManager(Class<T> clz) {
        KylinConfig base = base();
        if (base != this)
            return base.getManager(clz);

        Object mgr = managersCache == null ? null : managersCache.get(clz);
        if (mgr != null)
            return (T) mgr;

        synchronized (this) {
            if (managersCache == null)
                managersCache = new ConcurrentHashMap<>();

            mgr = managersCache.get(clz);
            if (mgr != null)
                return (T) mgr;

            try {
                // new manager via static Manager.newInstance()
                Method method = clz.getDeclaredMethod("newInstance", KylinConfig.class);
                method.setAccessible(true); // override accessibility
                mgr = method.invoke(null, this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            managersCache.put(clz, mgr);
        }
        return (T) mgr;
    }

    public <T> T getManager(String project, Class<T> clz) {
        KylinConfig base = base();
        if (base != this)
            return base.getManager(project, clz);

        ConcurrentHashMap<String, Object> mgrMap = (null == managersByPrjCache) ? null : managersByPrjCache.get(clz);
        Object mgr = (null == mgrMap) ? null : mgrMap.get(project);
        if (mgr != null)
            return (T) mgr;

        synchronized (this) {
            if (null == managersByPrjCache)
                managersByPrjCache = new ConcurrentHashMap<>();

            mgrMap = managersByPrjCache.get(clz);
            if (mgrMap == null)
                mgrMap = new ConcurrentHashMap<>();

            mgr = mgrMap.get(project);
            if (mgr != null)
                return (T) mgr;

            try {
                // new manager via static Manager.newInstance()
                Method method = clz.getDeclaredMethod("newInstance", KylinConfig.class, String.class);
                method.setAccessible(true); // override accessibility
                mgr = method.invoke(null, this, project);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            mgrMap.put(project, mgr);
            managersByPrjCache.put(clz, mgrMap);
        }
        return (T) mgr;
    }

    public void clearManagers() {
        KylinConfig base = base();
        if (base != this) {
            base.clearManagers();
            return;
        }

        if (managersCache != null)
            managersCache.clear();
    }

    public Properties exportToProperties() {
        Properties all = getAllProperties();
        Properties copy = new Properties();
        copy.putAll(all);
        return copy;
    }

    public String exportAllToString() throws IOException {
        final Properties allProps = getProperties(null);
        final OrderedProperties orderedProperties = KylinConfig.buildSiteOrderedProps();

        for (Map.Entry<Object, Object> entry : allProps.entrySet()) {
            String key = entry.getKey().toString();
            String value = entry.getValue().toString();
            if (!orderedProperties.containsProperty(key)) {
                orderedProperties.setProperty(key, value);
            } else if (!orderedProperties.getProperty(key).equalsIgnoreCase(value)) {
                orderedProperties.setProperty(key, value);
            }
        }

        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<String, String> entry : orderedProperties.entrySet()) {
            sb.append(entry.getKey() + "=" + entry.getValue()).append('\n');
        }
        return sb.toString();

    }

    public String exportToString() throws IOException {
        return exportToString(null); // null means to export all
    }

    public String exportToString(Collection<String> propertyKeys) throws IOException {
        Properties filteredProps = getProperties(propertyKeys);
        OrderedProperties orderedProperties = KylinConfig.buildSiteOrderedProps();

        if (propertyKeys != null) {
            for (String key : propertyKeys) {
                if (!filteredProps.containsKey(key)) {
                    filteredProps.put(key, orderedProperties.getProperty(key, ""));
                }
            }
        }

        final StringBuilder sb = new StringBuilder();
        for (Map.Entry<Object, Object> entry : filteredProps.entrySet()) {
            sb.append(entry.getKey() + "=" + entry.getValue()).append('\n');
        }
        return sb.toString();
    }

    public void exportToFile(File file) throws IOException {
        FileOutputStream fos = null;
        try {
            fos = new FileOutputStream(file);
            getAllProperties().store(fos, file.getAbsolutePath());
        } finally {
            IOUtils.closeQuietly(fos);
        }
    }

    public synchronized void reloadFromSiteProperties() {
        reloadKylinConfig(buildSiteProperties());
    }

    public KylinConfig base() {
        return this;
    }

    private int superHashCode() {
        return super.hashCode();
    }

    @Override
    public int hashCode() {
        return base().superHashCode();
    }

    @Override
    public boolean equals(Object another) {
        if (!(another instanceof KylinConfig))
            return false;
        else
            return this.base() == ((KylinConfig) another).base();
    }

}
