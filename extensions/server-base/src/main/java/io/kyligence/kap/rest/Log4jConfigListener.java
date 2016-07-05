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
import org.apache.kylin.common.util.DateFormat;
import org.apache.kylin.common.util.Pair;

import io.kyligence.kap.common.util.License;

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
        gatherLicense(kylinHome);
        gatherCommits(kylinHome);
        gatherEnv();
    }

    private void gatherEnv() {
        CliCommandExecutor cmd = new CliCommandExecutor();
        try {
            Pair<Integer, String> r = cmd.execute("hostname", null);
            if (r.getFirst() != 0) {
                License.error("Failed to get hostname, rc=" + r.getFirst());
            } else {
                String s = r.getSecond().trim();
                License.info("hostname=" + s);
                System.setProperty("hostname", s);
            }
        } catch (IOException ex) {
            License.error("Failed to get hostname", ex);
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
                        System.setProperty("kap.license.statement", statement);

                        String version = in.readLine();
                        System.setProperty("kap.version", version);
                        
                        String dates = in.readLine();
                        System.setProperty("kap.dates", dates);

                        String license = in.readLine();
                        System.setProperty("kap.license", license);

                        License.info("KAP License:\n" + statement + "====\n" + version + "\n" + dates + "\n" + license);

                        String[] split = dates.split(",");
                        long effectDate = DateFormat.stringToMillis(split[0]);
                        long expDate = DateFormat.stringToMillis(split[1]);
                        long oneDay = 1000 * 3600 * 24;
                        if (System.currentTimeMillis() < effectDate - oneDay) {
                            License.error("License is not effective yet.");
                        }
                        if (System.currentTimeMillis() > expDate + oneDay) {
                            License.error("License has expired.");
                        }
                        break;
                    }
                    statement += l + "\n";
                }
                in.close();
            } catch (IOException ex) {
                License.error("", ex);
            }
        } else {
            License.error("Missing license file");
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
                        License.info("kylin.commit=" + commit);
                        System.setProperty("kylin.commit", commit);
                    }
                    if (l.endsWith("@KAP")) {
                        String commit = l.substring(0, l.length() - 4);
                        License.info("kap.commit=" + commit);
                        System.setProperty("kap.commit", commit);
                    }
                }
                in.close();
            } catch (IOException ex) {
                License.error("", ex);
            }
        } else {
            License.error("Missing commit file");
        }
    }

}
