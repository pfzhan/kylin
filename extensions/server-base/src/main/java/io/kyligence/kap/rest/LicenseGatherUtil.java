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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LicenseGatherUtil {
    private static Logger licenseLog = LoggerFactory.getLogger("License");

    public static File getDefaultLicenseFile() {
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        File[] listFiles = kylinHome.listFiles(new FilenameFilter() {
            @Override
            public boolean accept(File dir, String name) {
                return name.equals("LICENSE");
            }
        });
        if (listFiles.length > 0) {
            return listFiles[0];
        }

        return null;
    }

    public static File getDefaultCommitFile() {
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        File commitFile = new File(kylinHome, "commit_SHA1");
        return commitFile;
    }

    public static File getDefaultVersionFile() {
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        File versionFile = new File(kylinHome, "VERSION");
        return versionFile;
    }

    public static void gatherLicenseInfo(File licenseFile, File commitFile, File versionFile, UUID prefix) {
        gatherLicense(licenseFile, prefix);
        gatherCommits(commitFile, prefix);
        gatherEnv(prefix);
        gatherVersion(versionFile, prefix);
        gatherMetastore(prefix);
        checkParallelScale(prefix);
    }

    private static void gatherMetastore(UUID prefix) {
        try {
            ResourceStore store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            String metaStoreId = store.getMetaStoreUUID();
            setProperty("kap.metastore", prefix, metaStoreId);
        } catch (Exception e) {
            licenseLog.error("Cannot get metastore uuid", e);
        }
    }

    private static void gatherVersion(File vfile, UUID prefix) {
        if (vfile.exists()) {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(vfile), "UTF-8"));
                String l;
                while ((l = in.readLine()) != null) {
                    setProperty("kap.version", prefix, l);
                    licenseLog.info("KAP Version: " + l + "\n");
                    break;
                }
                in.close();
            } catch (IOException ex) {
                licenseLog.error("", ex);
            }
        }
    }

    private static void gatherEnv(UUID prefix) {
        CliCommandExecutor cmd = new CliCommandExecutor();
        try {
            Pair<Integer, String> r = cmd.execute("hostname", null);
            if (r.getFirst() != 0) {
                licenseLog.error("Failed to get hostname, rc=" + r.getFirst());
            } else {
                String s = r.getSecond().trim();
                licenseLog.info("hostname=" + s);
                setProperty("hostname", prefix, s);
            }
        } catch (IOException ex) {
            licenseLog.error("Failed to get hostname", ex);
        }
    }

    private static void gatherLicense(File lfile, UUID prefix) {
        if (lfile == null || !lfile.exists()) {
            return; //license file is allowed to be missing
        }

        try {
            BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(lfile), "UTF-8"));
            String statement = "";
            String l;
            while ((l = in.readLine()) != null) {
                if ("====".equals(l)) {
                    setProperty("kap.license.statement", prefix, statement);

                    String version = in.readLine();
                    setProperty("kap.license.version", prefix, version);

                    String dates = in.readLine();
                    setProperty("kap.dates", prefix, dates);

                    String license = in.readLine();
                    setProperty("kap.license", prefix, license);

                    licenseLog.info("KAP License:\n" + statement + "====\n" + version + "\n" + dates + "\n" + license);
                    break;
                }
                statement += l + "\n";
            }
            in.close();
        } catch (IOException ex) {
            licenseLog.error("", ex);
        }
    }

    private static void gatherCommits(File commitFile, UUID prefix) {
        if (commitFile != null && commitFile.exists()) {
            try {
                BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(commitFile), "UTF-8"));
                String l;
                while ((l = in.readLine()) != null) {
                    if (l.endsWith("@ApacheKylin")) {
                        String commit = l.substring(0, l.length() - 12);
                        licenseLog.info("kylin.commit=" + commit);
                        setProperty("kylin.commit", prefix, commit);
                    }
                    if (l.endsWith("@KAP")) {
                        String commit = l.substring(0, l.length() - 4);
                        licenseLog.info("kap.commit=" + commit);
                        setProperty("kap.commit", prefix, commit);
                    }
                }
                in.close();
            } catch (IOException ex) {
                licenseLog.error("", ex);
            }
        }
    }

    private static void checkParallelScale(UUID prefix) {
        String lic = getProperty("kap.license.statement", prefix);
        int parallel = -1;
        try {
            if (lic != null) {
                BufferedReader reader = new BufferedReader(new StringReader(lic));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.startsWith("Parallel Scale:")) {
                        parallel = Integer.parseInt(line.substring("Parallel Scale:".length()).trim());
                    }
                }
                reader.close();
            }
        } catch (IOException e) {
            // ignore
        }

        if (parallel > 0) {
            Executors.newScheduledThreadPool(1).scheduleAtFixedRate(new ParallelScaleChecker(parallel), 5, 10,
                    TimeUnit.MINUTES);
        }
    }

    public static void setProperty(String key, UUID keyPrefix, String value) {
        System.setProperty((keyPrefix == null ? "" : keyPrefix) + key, value);
    }

    public static String getProperty(String key, UUID keyPrefix) {
        return System.getProperty((keyPrefix == null ? "" : keyPrefix) + key);
    }

}
