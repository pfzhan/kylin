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

package io.kyligence.kap.rest;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import org.apache.catalina.Context;
import org.apache.catalina.core.AprLifecycleListener;
import org.apache.catalina.core.StandardServer;
import org.apache.catalina.deploy.ErrorPage;
import org.apache.catalina.startup.Tomcat;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;

public class KAPDebugTomcat {

    public static void setupDebugEnv() {
        try {
            System.setProperty("HADOOP_USER_NAME", "root");
            System.setProperty("log4j.configuration", "file:../../build/conf/kylin-tools-log4j.properties");

            // test_case_data/sandbox/ contains HDP 2.2 site xmls which is dev sandbox
            KylinConfig.setSandboxEnvIfPossible();
            overrideDevJobJarLocations();

            System.setProperty("spring.profiles.active", "testing");
            System.setProperty("kylin.query.cache-enabled", "false");

            //avoid log permission issue
            if (System.getProperty("catalina.home") == null)
                System.setProperty("catalina.home", ".");

            if (StringUtils.isEmpty(System.getProperty("hdp.version"))) {
                System.setProperty("hdp.version", "2.4.0.0-169");
            }

            // workaround for job submission from win to linux -- https://issues.apache.org/jira/browse/MAPREDUCE-4052
            if (Shell.WINDOWS) {
                {
                    Field field = Shell.class.getDeclaredField("WINDOWS");
                    field.setAccessible(true);
                    Field modifiersField = Field.class.getDeclaredField("modifiers");
                    modifiersField.setAccessible(true);
                    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
                    field.set(null, false);
                }
                {
                    Field field = java.io.File.class.getDeclaredField("pathSeparator");
                    field.setAccessible(true);
                    Field modifiersField = Field.class.getDeclaredField("modifiers");
                    modifiersField.setAccessible(true);
                    modifiersField.setInt(field, field.getModifiers() & ~Modifier.FINAL);
                    field.set(null, ":");
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void overrideDevJobJarLocations() {
        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        File devJobJar = findFile("../assembly/target", "kap-assembly-.*-SNAPSHOT-job.jar");
        File sparkJar = findFile("../storage-parquet/target", "kap-storage-parquet-.*-SNAPSHOT-spark.jar");
        try {
            String sparkJarCanonicalPath = sparkJar.getCanonicalPath();
            System.setProperty("kap.query.engine.sparder-additional-jars", sparkJarCanonicalPath);
        } catch (IOException e) {
            e.printStackTrace();
        }
        if (devJobJar != null) {
            conf.overrideMRJobJarPath(devJobJar.getAbsolutePath());
        }
        File devCoprocessorJar = findFile("../storage-hbase/target", "kap-storage-hbase-.*-SNAPSHOT-coprocessor.jar");
        if (devCoprocessorJar != null) {
            conf.overrideCoprocessorLocalJar(devCoprocessorJar.getAbsolutePath());
        }
    }

    private static File findFile(String dir, String ptn) {
        File[] files = new File(dir).listFiles();
        if (files != null) {
            for (File f : files) {
                if (f.getName().matches(ptn))
                    return f;
            }
        }
        return null;
    }

    public static void main(String[] args) throws Exception {
        setupDebugEnv();

        int port = 7070;
        if (args.length >= 1) {
            port = Integer.parseInt(args[0]);
        }

        File webBase = new File("../../webapp/app");
        File webInfDir = new File(webBase, "WEB-INF");
        File metaInfDir = new File(webBase, "META-INF");
        FileUtils.deleteDirectory(webInfDir);
        FileUtils.deleteDirectory(metaInfDir);
        FileUtils.copyDirectoryToDirectory(new File("../server/src/main/webapp/WEB-INF"), webBase);
        FileUtils.copyDirectoryToDirectory(new File("../examples/test_case_data/webapps/META-INF"), webBase);

        Tomcat tomcat = new Tomcat();
        tomcat.setPort(port);
        tomcat.setBaseDir(".");

        // Add AprLifecycleListener
        StandardServer server = (StandardServer) tomcat.getServer();
        AprLifecycleListener listener = new AprLifecycleListener();
        server.addLifecycleListener(listener);

        Context webContext = tomcat.addWebapp("/kylin", webBase.getAbsolutePath());
        ErrorPage notFound = new ErrorPage();
        notFound.setErrorCode(404);
        notFound.setLocation("/index.html");
        webContext.addErrorPage(notFound);
        webContext.addWelcomeFile("index.html");
        // tomcat start
        tomcat.start();
        tomcat.getServer().await();
    }

}
