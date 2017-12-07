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

package io.kyligence.kap.ext.classloader;

import static io.kyligence.kap.ext.classloader.ClassLoaderUtils.findFile;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkClassLoader extends URLClassLoader {
    //preempt these classes from parent
    private static String[] SPARK_CL_PREEMPT_CLASSES = new String[] { "org.apache.spark", "scala.",
            "org.spark_project" };

    //preempt these files from parent
    private static String[] SPARK_CL_PREEMPT_FILES = new String[] { "spark-version-info.properties", "HiveClientImpl",
            "org/apache/spark" };

    //when loading class (indirectly used by SPARK_CL_PREEMPT_CLASSES), some of them should NOT use parent's first
    private static String[] THIS_CL_PRECEDENT_CLASSES = new String[] { "javax.ws.rs", "org.apache.hadoop.hive" };

    //when loading class (indirectly used by SPARK_CL_PREEMPT_CLASSES), some of them should use parent's first
    private static String[] PARENT_CL_PRECEDENT_CLASSES = new String[] {
            //            // Java standard library:
            "com.sun.", "launcher.", "java.", "javax.", "org.ietf", "org.omg", "org.w3c", "org.xml", "sunw.", "sun.",
            // logging
            "org.apache.commons.logging", "org.apache.log4j", "org.slf4j", "org.apache.hadoop",
            // Hadoop/HBase/ZK:
            "io.kyligence", "org.apache.kylin", "com.intellij", "org.apache.calcite" };

    private static final Set<String> classNotFoundCache = new HashSet<>();
    private static Logger logger = LoggerFactory.getLogger(SparkClassLoader.class);

    static {
        String sparkclassloader_spark_cl_preempt_classes = System.getenv("SPARKCLASSLOADER_SPARK_CL_PREEMPT_CLASSES");
        if (!StringUtils.isEmpty(sparkclassloader_spark_cl_preempt_classes)) {
            SPARK_CL_PREEMPT_CLASSES = StringUtils.split(sparkclassloader_spark_cl_preempt_classes, ",");
        }

        String sparkclassloader_spark_cl_preempt_files = System.getenv("SPARKCLASSLOADER_SPARK_CL_PREEMPT_FILES");
        if (!StringUtils.isEmpty(sparkclassloader_spark_cl_preempt_files)) {
            SPARK_CL_PREEMPT_FILES = StringUtils.split(sparkclassloader_spark_cl_preempt_files, ",");
        }

        String sparkclassloader_this_cl_precedent_classes = System.getenv("SPARKCLASSLOADER_THIS_CL_PRECEDENT_CLASSES");
        if (!StringUtils.isEmpty(sparkclassloader_this_cl_precedent_classes)) {
            THIS_CL_PRECEDENT_CLASSES = StringUtils.split(sparkclassloader_this_cl_precedent_classes, ",");
        }

        String sparkclassloader_parent_cl_precedent_classes = System
                .getenv("SPARKCLASSLOADER_PARENT_CL_PRECEDENT_CLASSES");
        if (!StringUtils.isEmpty(sparkclassloader_parent_cl_precedent_classes)) {
            PARENT_CL_PRECEDENT_CLASSES = StringUtils.split(sparkclassloader_parent_cl_precedent_classes, ",");
        }

        try {
            final Method registerParallel = ClassLoader.class.getDeclaredMethod("registerAsParallelCapable");
            AccessController.doPrivileged(new PrivilegedAction<Object>() {
                public Object run() {
                    registerParallel.setAccessible(true);
                    return null;
                }
            });
            Boolean result = (Boolean) registerParallel.invoke(null);
            if (!result) {
                logger.warn("registrationFailed");
            }
        } catch (Exception ignore) {

        }
    }

    /**
     * Creates a DynamicClassLoader that can load classes dynamically
     * from jar files under a specific folder.
     *
     * @param parent the parent ClassLoader to set.
     */
    protected SparkClassLoader(ClassLoader parent) throws IOException {
        super(new URL[] {}, parent);
        init();
    }

    public void init() throws MalformedURLException {
        String spark_home = System.getenv("SPARK_HOME");
        if (spark_home == null) {
            spark_home = System.getProperty("SPARK_HOME");
            if (spark_home == null) {
                throw new RuntimeException(
                        "Spark home not found; set it explicitly or use the SPARK_HOME environment variable.");
            }
        }
        File file = new File(spark_home + "/jars");
        File[] jars = file.listFiles();
        for (File jar : jars) {
            addURL(jar.toURI().toURL());
        }
        if (System.getenv("KYLIN_HOME") != null) {
            // for prod
            String kylin_home = System.getenv("KYLIN_HOME");
            File sparkJar = findFile(kylin_home + "/lib", "kylin-udf-.*-SNAPSHOT.jar");
            if (sparkJar != null) {
                logger.info("Add kylin UDF jar to spark classloader : " + sparkJar.getName());
                addURL(sparkJar.toURI().toURL());
            }
        } else if (Files.exists(Paths.get("../udf/target/classes"))) {
            //  for debugtomcat
            logger.info("Add kylin UDF classes to spark classloader");
            addURL(new File("../udf/target/classes").toURI().toURL());
        } else {
            if (System.getenv("KYLIN_HOME") == null) {
                throw new RuntimeException(
                        "Can not found kylin UDF jar, please set KYLIN_HOME and make sure the kylin-udf-*.jar exists in $KYLIN_HOME/lib");
            } else {
                throw new RuntimeException("Can not found kylin UDF classes, please run cmd mvn compile");
            }
        }

    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {

        if (needToUseGlobal(name)) {
            logger.debug("delegate " + name + " directly to parent");
            return super.loadClass(name, resolve);
        }
        return doLoadclass(name);
    }

    private Class<?> doLoadclass(String name) throws ClassNotFoundException {
        synchronized (getClassLoadingLock(name)) {
            // Check whether the class has already been loaded:
            Class<?> clasz = findLoadedClass(name);
            if (clasz != null) {
                logger.debug("Class " + name + " already loaded");
            } else {
                try {
                    // Try to find this class using the URLs passed to this ClassLoader
                    logger.debug("Finding class: " + name);
                    clasz = super.findClass(name);
                    if (clasz == null) {
                        logger.debug("cannot find class" + name);
                    }
                } catch (ClassNotFoundException e) {
                    classNotFoundCache.add(name);
                    // Class not found using this ClassLoader, so delegate to parent
                    logger.debug("Class " + name + " not found - delegating to parent");
                    try {
                        // sparder and query module has some class start with org.apache.spark,
                        // We need to use some lib that does not exist in spark/jars
                        clasz = getParent().loadClass(name);
                    } catch (ClassNotFoundException e2) {
                        // Class not found in this ClassLoader or in the parent ClassLoader
                        // Log some debug output before re-throwing ClassNotFoundException
                        logger.debug("Class " + name + " not found in parent loader");
                        throw e2;
                    }
                }
            }
            return clasz;
        }
    }

    private boolean isThisCLPrecedent(String name) {
        for (String exemptPrefix : THIS_CL_PRECEDENT_CLASSES) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean isParentCLPrecedent(String name) {
        for (String exemptPrefix : PARENT_CL_PRECEDENT_CLASSES) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    private boolean needToUseGlobal(String name) {
        return !isThisCLPrecedent(name) && !classNeedPreempt(name) && isParentCLPrecedent(name);
    }

    boolean classNeedPreempt(String name) {
        if (classNotFoundCache.contains(name)) {
            return false;
        }
        for (String exemptPrefix : SPARK_CL_PREEMPT_CLASSES) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    boolean fileNeedPreempt(String name) {
        for (String exemptPrefix : SPARK_CL_PREEMPT_FILES) {
            if (name.contains(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }
}
