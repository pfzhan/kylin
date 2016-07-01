package io.kyligence.kap.rest;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;

import javax.servlet.ServletContextEvent;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;

public class Log4jConfigListener extends org.springframework.web.util.Log4jConfigListener {

    private boolean isDebugTomcat;

    public Log4jConfigListener() {
        KapConfig config = KapConfig.getInstanceFromEnv();
        this.isDebugTomcat = config.isDevEnv();
    }

    @Override
    public void contextInitialized(ServletContextEvent event) {
        if (!isDebugTomcat) {
            super.contextInitialized(event);
        }
        gatherLicenseInfo();
    }

    @Override
    public void contextDestroyed(ServletContextEvent event) {
        if (!isDebugTomcat) {
            super.contextDestroyed(event);
        }
    }

    private void gatherLicenseInfo() {
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        gatherEnv();
        gatherCommits(kylinHome);
        gatherLicense(kylinHome);
    }

    private void gatherEnv() {
        CliCommandExecutor cmd = new CliCommandExecutor();
        try {
            Pair<Integer, String> r = cmd.execute("hostname", null);
            if (r.getFirst() != 0) {
                System.out.println("ERROR: Failed to get hostname, rc=" + r.getFirst());
            } else {
                String s = r.getSecond().trim();
                System.out.println("hostname=" + s);
                System.setProperty("hostname", s);
            }
        } catch (IOException e) {
            System.out.println("ERROR: Failed to get hostname");
        }
        
    }

    private void gatherLicense(File kylinHome) {
        File[] listFiles = kylinHome.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.endsWith(".license");
            }
        });
        if (listFiles.length > 0) {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(listFiles[0]), "UTF-8"));
                String statement = "";
                String l;
                while ((l = in.readLine()) != null) {
                    if ("====".equals(l)) {
                        l = in.readLine();
                        System.out.println("kap.dates=" + l);
                        System.setProperty("kap.dates", l);
                        l = in.readLine();
                        System.out.println("kap.license=" + l);
                        System.setProperty("kap.license", l);
                        break;
                    }
                    statement += l + "\n";
                }
                in.close();
                System.out.println(statement);
                System.setProperty("kap.license.statement", statement);
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

    private void gatherCommits(File kylinHome) {
        File commitFile = new File(kylinHome, "commit_SHA1");
        if (commitFile.exists()) {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(commitFile), "UTF-8"));
                String l;
                while ((l = in.readLine()) != null) {
                    if (l.endsWith("@ApacheKylin")) {
                        String commit = l.substring(0, l.length() - 12);
                        System.out.println("kylin.commit=" + commit);
                        System.setProperty("kylin.commit", commit);
                    }
                    if (l.endsWith("@KAP")) {
                        String commit = l.substring(0, l.length() - 4);
                        System.out.println("kap.commit=" + commit);
                        System.setProperty("kap.commit", commit);
                    }
                }
                in.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }

}
