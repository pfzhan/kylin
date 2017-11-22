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

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLClassLoader;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkClassLoader extends URLClassLoader {
    private static final String[] CLASS_PREFIX = new String[] { "org.apache.spark", "scala.", "org.spark_project"
            //            "javax.ws.rs.core.Application",
            //            "javax.ws.rs.core.UriBuilder", "org.glassfish.jersey", "javax.ws.rs.ext"
            //user javax.ws.rs.api 2.01  not jersey-core-1.9.jar
    };
    private static final String[] RESOURCE_PREFIX = new String[] { "spark-version-info.properties", "HiveClientImpl" };
    private static final String[] CLASS_PREFIX_EXEMPTIONS = new String[] {
            //            // Java standard library:
            "com.sun.", "launcher.", "java.", "javax.", "org.ietf", "org.omg", "org.w3c", "org.xml", "sunw.", "sun.",
            // logging
            "org.apache.commons.logging", "org.apache.log4j", "org.slf4j", "org.apache.hadoop",
            // Hadoop/HBase/ZK:
            "io.kyligence", "org.apache.kylin", "com.intellij", "org.apache.calcite" };
    private static final Set<String> classNotFoundCache = new HashSet<>();
    private static Logger logger = LoggerFactory.getLogger(SparkClassLoader.class);

    static {
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

    //    @Override
    //    public InputStream getResourceAsStream(String name) {
    //        if (RESOURCE.contains(name)) {
    //            return getParent().getResourceAsStream(name);
    //        }
    //        return super.getResourceAsStream(name);
    //    }

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
    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (!name.startsWith("org.apache.hadoop.hive") && !name.startsWith("javax.ws.rs") && isClassExempt(name)
                && !needLoad(name)) {
            logger.debug("Skipping exempt class " + name + " - delegating directly to parent");
            try {
                return getParent().loadClass(name);
            } catch (ClassNotFoundException e) {
                return super.findClass(name);
            }
        }
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

    //    @Override
    //    protected Class<?> findClass(String name) throws ClassNotFoundException {
    //        Class clazz = null;
    //           if(classloader.needLoad(name)) {
    //            clazz =super.findClass(name);
    //           }
    //       if(clazz != null){
    //           return clazz;
    //       }
    //        return Class.forName(name, false, parent);
    //    }

    protected boolean isClassExempt(String name) {
        for (String exemptPrefix : CLASS_PREFIX_EXEMPTIONS) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    public boolean needLoad(String name) {
        if (classNotFoundCache.contains(name)) {
            return false;
        }
        for (String exemptPrefix : CLASS_PREFIX) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    public boolean haResource(String name) {
        if (name.replaceAll("/", "\\.").startsWith("org.apache.spark")) {
            return true;
        }

        for (String exemptPrefix : RESOURCE_PREFIX) {
            if (name.contains(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }
}
