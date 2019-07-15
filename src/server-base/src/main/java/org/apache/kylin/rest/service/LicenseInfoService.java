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

package org.apache.kylin.rest.service;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.model.LicenseInfo;
import org.apache.kylin.rest.msg.Message;
import org.apache.kylin.rest.msg.MsgPicker;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

import io.kyligence.kap.rest.config.initialize.AppInitializedEvent;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Service("licenseInfoService")
public class LicenseInfoService extends BasicService {
    private static final String UNLIMITED = "Unlimited";

    public static final String CODE_WARNING = "001";
    public static final String CODE_ERROR = "002";

    public static final String LICENSE_FILENAME = "LICENSE";
    public static final String HOSTNAME = "hostname";
    public static final String KE_COMMIT = "ke.commit";
    public static final String KE_VERSION = "ke.version";
    public static final String KE_METASTORE = "ke.metastore";
    public static final String KE_DATES = "ke.dates";
    public static final String KE_LICENSE = "ke.license";
    public static final String KE_LICENSE_LEVEL = "ke.license.level";
    public static final String KE_LICENSE_CATEGORY = "ke.license.category";
    public static final String KE_LICENSE_STATEMENT = "ke.license.statement";
    public static final String KE_LICENSE_ISEVALUATION = "ke.license.isEvaluation";
    public static final String KE_LICENSE_SERVICEEND = "ke.license.serviceEnd";
    public static final String KE_LICENSE_NODES = "ke.license.nodes";
    public static final String KE_LICENSE_ISCLOUD = "ke.license.isCloud";
    public static final String KE_LICENSE_INFO = "ke.license.info";
    public static final String KE_LICENSE_VERSION = "ke.license.version";
    public static final String KE_LICENSE_VOLUME = "ke.license.volume";

    public static File getDefaultLicenseFile() {
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        File[] listFiles = kylinHome.listFiles((dir, name) -> name.equals(LICENSE_FILENAME));
        if (listFiles.length > 0) {
            return listFiles[0];
        }

        return null;
    }

    public static File getDefaultCommitFile() {
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        return new File(kylinHome, "commit_SHA1");
    }

    public static File getDefaultVersionFile() {
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        return new File(kylinHome, "VERSION");
    }

    public static String getProperty(String key, UUID keyPrefix) {
        return System.getProperty((keyPrefix == null ? "" : keyPrefix) + key);
    }

    @EventListener(AppInitializedEvent.class)
    public void init() {
        init(code -> System.exit(code));
    }

    void init(Consumer<Integer> onError) {
        try {
            gatherLicenseInfo(getDefaultLicenseFile(), getDefaultCommitFile(), getDefaultVersionFile(), null);
            val info = extractLicenseInfo();
            verifyLicense(info);
        } catch (Exception e) {
            log.error("license is invalid", e);
            onError.accept(1);
        }
    }

    public LicenseInfo extractLicenseInfo() {
        val result = new LicenseInfo();
        result.setStatement(System.getProperty(KE_LICENSE_STATEMENT));
        result.setVersion(System.getProperty(KE_VERSION));
        result.setDates(System.getProperty(KE_DATES));
        result.setCommit(System.getProperty(KE_COMMIT));

        if ("true".equals(System.getProperty(KE_LICENSE_ISEVALUATION))) {
            result.setEvaluation(true);
        }
        if ("true".equals(System.getProperty(KE_LICENSE_ISCLOUD))) {
            result.setEvaluation(true);
        }

        if (!StringUtils.isEmpty(System.getProperty(KE_LICENSE_SERVICEEND))) {
            result.setServiceEnd(System.getProperty(KE_LICENSE_SERVICEEND));
        }

        result.setNodes(System.getProperty(KE_LICENSE_NODES));
        result.setVolume(System.getProperty(KE_LICENSE_VOLUME));
        result.setInfo(System.getProperty(KE_LICENSE_INFO));
        result.setLevel(System.getProperty(KE_LICENSE_LEVEL));
        result.setCategory(System.getProperty(KE_LICENSE_CATEGORY));

        return result;
    }

    public String verifyLicense(LicenseInfo info) {
        return null;
    }

    public void gatherLicenseInfo(File licenseFile, File commitFile, File versionFile, UUID prefix) {
        gatherLicense(licenseFile, prefix);
        gatherCommits(commitFile, prefix);
        gatherEnv(prefix);
        gatherVersion(versionFile, prefix);
        gatherMetastore(prefix);
        gatherStatementInfo(prefix);
    }

    private void gatherMetastore(UUID prefix) {
        try {
            ResourceStore store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
            String metaStoreId = store.getMetaStoreUUID();
            setProperty(KE_METASTORE, prefix, metaStoreId);
        } catch (Exception e) {
            log.error("Cannot get metastore uuid", e);
        }
    }

    private void gatherVersion(File vfile, UUID prefix) {
        if (vfile.exists()) {
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(new FileInputStream(vfile), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    setProperty(KE_VERSION, prefix, line);
                    log.info("Kyligence Enterprise Version: " + line + "\n");
                    break;
                }
            } catch (IOException ex) {
                log.error("", ex);
            }
        }
    }

    private void gatherEnv(UUID prefix) {
        CliCommandExecutor cmd = new CliCommandExecutor();
        try {
            Pair<Integer, String> r = cmd.execute(HOSTNAME, null);
            if (r.getFirst() != 0) {
                log.error("Failed to get hostname, rc=" + r.getFirst());
            } else {
                String s = r.getSecond().trim();
                log.info("hostname=" + s);
                setProperty(HOSTNAME, prefix, s);
            }
        } catch (ShellException ex) {
            log.error("Failed to get hostname", ex);
        }
    }

    private void gatherLicense(File lfile, UUID prefix) {
        if (lfile == null || !lfile.exists()) {
            return; //license file is allowed to be missing
        }

        try (val in = Files.newBufferedReader(lfile.toPath(), StandardCharsets.UTF_8)) {
            StringBuilder statement = new StringBuilder();
            String l;
            while ((l = in.readLine()) != null) {
                if ("====".equals(l)) {
                    setProperty(KE_LICENSE_STATEMENT, prefix, statement.toString());

                    String version = in.readLine();
                    setProperty(KE_LICENSE_VERSION, prefix, version);

                    String dates = in.readLine();
                    setProperty(KE_DATES, prefix, dates);

                    String license = in.readLine();
                    setProperty(KE_LICENSE, prefix, license);

                    log.info("Kyligence Enterprise License:\n" + statement + "====\n" + version + "\n" + dates + "\n"
                            + license);
                    break;
                }
                statement.append(l).append("\n");
            }
        } catch (IOException ex) {
            log.error("", ex);
        }
    }

    private void gatherCommits(File commitFile, UUID prefix) {
        if (commitFile == null || !commitFile.exists()) {
            return;
        }
        try (val in = Files.newBufferedReader(commitFile.toPath(), StandardCharsets.UTF_8)) {
            String line;
            while ((line = in.readLine()) != null) {
                if (line.endsWith("@KAP")) {
                    String commit = line.substring(0, line.length() - 4);
                    log.info("{}={}", KE_COMMIT, commit);
                    setProperty(KE_COMMIT, prefix, commit);
                }
            }
        } catch (IOException ex) {
            log.error("", ex);
        }
    }

    private void gatherStatementInfo(UUID prefix) {
        String statement = System.getProperty(KE_LICENSE_STATEMENT);

        // set defaults
        setProperty(KE_LICENSE_ISEVALUATION, prefix, "false");
        setProperty(KE_LICENSE_CATEGORY, prefix, "4.x");
        setProperty(KE_LICENSE_LEVEL, prefix, "professional");

        if (statement == null) {
            return;
        }
        BiFunction<String, String, Optional<String>> extractValue = (line, target) -> {
            if (line.contains(target)) {
                return Optional.of(line.substring(target.length()).trim());
            }
            return Optional.empty();
        };
        try (val reader = new BufferedReader(new StringReader(statement))) {
            String line;
            AtomicReference<String> volume = new AtomicReference<>();
            AtomicReference<String> node = new AtomicReference<>();
            int lineNum = 0;

            while ((line = reader.readLine()) != null) {
                if (lineNum == 0 && line.toLowerCase().contains("license")) {
                    setProperty(KE_LICENSE_INFO, prefix, line);
                }
                if (line.toLowerCase().contains("evaluation")) {
                    setProperty(KE_LICENSE_ISEVALUATION, prefix, "true");
                }
                if (line.toLowerCase().contains("for cloud")) {
                    setProperty(KE_LICENSE_ISCLOUD, prefix, "true");
                }
                extractValue.apply(line, "Service End:").ifPresent(v -> setProperty(KE_LICENSE_SERVICEEND, prefix, v));
                extractValue.apply(line, "Category:").ifPresent(v -> setProperty(KE_LICENSE_CATEGORY, prefix, v));
                extractValue.apply(line, "Level:").ifPresent(v -> setProperty(KE_LICENSE_LEVEL, prefix, v));
                extractValue.apply(line, "Volume:").ifPresent(volume::set);
                extractValue.apply(line, "Service Nodes:").ifPresent(node::set);
                lineNum++;
            }

            checkLicenseInfo(volume.get(), node.get(), prefix);

        } catch (IOException e) {
            // ignore
        }
    }

    private void checkLicenseInfo(String volume, String node, UUID prefix) throws IOException {
        String realVolume = getRealNode(volume, Double::parseDouble);
        String realNode = getRealNode(node, Long::parseLong);

        setProperty(KE_LICENSE_VOLUME, prefix, realVolume);
        setProperty(KE_LICENSE_NODES, prefix, realNode);
    }

    private String getRealNode(String node, Consumer<String> checker) throws IOException {
        Message msg = MsgPicker.getMsg();
        String realNode;
        if (StringUtils.isBlank(node)) {
            realNode = UNLIMITED;
        } else {
            if (!UNLIMITED.equals(node)) {
                try {
                    checker.accept(node);
                } catch (NumberFormatException e) {
                    backupAndDeleteLicense("error");
                    throw new BadRequestException(msg.getLICENSE_INVALID_LICENSE(), CODE_ERROR, e);
                }
            }
            realNode = node;
        }
        return realNode;
    }

    File backupAndDeleteLicense(String type) throws IOException {
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        File licenseFile = new File(kylinHome, LICENSE_FILENAME);
        if (licenseFile.exists()) {
            File licenseBackFile = new File(kylinHome, "LICENSE." + type);
            if (licenseBackFile.exists())
                FileUtils.forceDelete(licenseBackFile);
            FileUtils.copyFile(licenseFile, licenseBackFile);
            FileUtils.forceDelete(licenseFile);
        }
        return licenseFile;
    }

    public void setProperty(String key, UUID keyPrefix, String value) {
        System.setProperty((keyPrefix == null ? "" : keyPrefix) + key, value);
    }
}
