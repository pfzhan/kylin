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

import static io.kyligence.kap.ext.classloader.ClassUtils.findFile;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.apache.catalina.loader.ParallelWebappClassLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DebugTomcatClassLoader extends ParallelWebappClassLoader {
    private static final String[] CLASS_PREFIX_EXEMPTIONS = new String[] {
            // Java standard library:
            "com.sun.", "launcher.", "javax.", "org.ietf", "java", "org.omg", "org.w3c", "org.xml", "sunw.",
            // logging
            "org.slf4j", "org.apache.commons.logging", "org.apache.log4j", "org.apache.catalina", "org.apache.tomcat" };
    private static final String[] CLASS_PREFIX_INCLUDE = new String[] { "io.kyligence", "org.apache.kylin",
            "org.apache.calcite" };
    private static final String[] CODE_GEN_CLASS = new String[] { "org.apache.spark.sql.catalyst.expressions.Object",
            "Baz" };
    private static Logger logger = LoggerFactory.getLogger(DebugTomcatClassLoader.class);
    private SparkClassLoader sparkClassLoader;

    /**
     * Creates a DynamicClassLoader that can load classes dynamically
     * from jar files under a specific folder.
     *
     * @param parent the parent ClassLoader to set.
     */
    public DebugTomcatClassLoader(ClassLoader parent) throws IOException {
        super(parent);
        sparkClassLoader = new SparkClassLoader(this);
        init();
    }

    public void init() {

        String classPath = System.getProperty("java.class.path");
        if (classPath == null) {
            throw new RuntimeException("");
        }

        String[] jars = classPath.split(":");
        for (String jar : jars) {
            try {
                URL url = new File(jar).toURI().toURL();
                addURL(url);
            } catch (MalformedURLException e) {
                e.printStackTrace();
            }
        }
        String spark_home = System.getenv("SPARK_HOME");
        try {
            //SparkContext use spi to match deploy mode
            //otherwise SparkContext init fail ,can not find yarn deploy mode
            File yarnJar = findFile(spark_home + "/jars", "spark-yarn.*.jar");
            addURL(yarnJar.toURI().toURL());
            //jersey in spark will attempt find @Path class file in current classloader. Not possible to delegate to spark loader
            // otherwise spark web ui executors tab can not render
            File coreJar = findFile(spark_home + "/jars", "spark-core.*.jar");
            addURL(coreJar.toURI().toURL());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

    }

    @Override
    public Class<?> loadClass(String name, boolean resolve) throws ClassNotFoundException {
        if (isCodeGen(name)) {
            throw new ClassNotFoundException();
        }
        if (sparkClassLoader.needLoad(name)) {
            return sparkClassLoader.loadClass(name);
        }
        if (isClassExempt(name)) {
            logger.debug("Skipping exempt class " + name + " - delegating directly to parent");
            return parent.loadClass(name);
        }
        return super.loadClass(name, resolve);
    }

    //    @Override
    //    public Class<?> findClass(String name) throws ClassNotFoundException {
    //
    //        Class<?> aClass = null;
    //        if (isInclude(name)) {
    //            aClass = super.findClass(name);
    //        }
    //        if (aClass == null) {
    //            return Class.forName(name, false, parent);
    //        }
    //        return aClass;
    //
    //    }

    @Override
    public InputStream getResourceAsStream(String name) {
        if (sparkClassLoader.haResource(name)) {
            return sparkClassLoader.getResourceAsStream(name);
        }
        return super.getResourceAsStream(name);

    }

    protected boolean isClassExempt(String name) {
        for (String exemptPrefix : CLASS_PREFIX_EXEMPTIONS) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    boolean isInclude(String name) {
        for (String exemptPrefix : CLASS_PREFIX_INCLUDE) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }

    boolean isCodeGen(String name) {
        for (String exemptPrefix : CODE_GEN_CLASS) {
            if (name.startsWith(exemptPrefix)) {
                return true;
            }
        }
        return false;
    }
}
