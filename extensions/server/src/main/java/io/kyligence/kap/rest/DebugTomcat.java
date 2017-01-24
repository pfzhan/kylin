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

import org.apache.catalina.Context;
import org.apache.catalina.core.AprLifecycleListener;
import org.apache.catalina.core.StandardServer;
import org.apache.catalina.deploy.ErrorPage;
import org.apache.catalina.startup.Tomcat;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.util.Shell;
import org.apache.kylin.common.KylinConfig;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

public class DebugTomcat {

    public static void setupDebugEnv() {
        try {
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
                throw new RuntimeException("No hdp.version set; Please set hdp.version in your jvm option, for example: -Dhdp.version=2.4.0.0-169");
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

        String webBase = new File("../../webapp/app").getAbsolutePath();
        if (new File(webBase, "WEB-INF").exists() == false) {
            throw new RuntimeException("In order to launch Kylin web app from IDE, please run:\ncp -rf extensions/server/src/main/webapp/WEB-INF webapp/app/");
        }

        Tomcat tomcat = new Tomcat();
        tomcat.setPort(port);
        tomcat.setBaseDir(".");

        // Add AprLifecycleListener
        StandardServer server = (StandardServer) tomcat.getServer();
        AprLifecycleListener listener = new AprLifecycleListener();
        server.addLifecycleListener(listener);

        Context webContext = tomcat.addWebapp("/kylin", webBase);
        ErrorPage notFound = new ErrorPage();
        notFound.setErrorCode(404);
        notFound.setLocation("/index.html");
        webContext.addErrorPage(notFound);
        webContext.addWelcomeFile("index.html");
//        webContext.getJarScanner().setJarScanFilter(new JarScanFilter() {
//            @Override
//            public boolean check(JarScanType arg0, String arg1) {
//                return false;
//            }
//        });
        // tomcat start
        tomcat.start();
        tomcat.getServer().await();
    }

}
