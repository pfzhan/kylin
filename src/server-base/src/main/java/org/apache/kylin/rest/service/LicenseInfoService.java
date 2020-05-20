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

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import io.kyligence.kap.common.license.Constants;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord;
import io.kyligence.kap.rest.cluster.ClusterManager;
import io.kyligence.kap.rest.config.initialize.AfterMetadataReadyEvent;
import io.kyligence.kap.rest.request.LicenseRequest;
import io.kyligence.kap.rest.response.CapacityDetailsResponse;
import io.kyligence.kap.rest.response.LicenseInfoWithDetailsResponse;
import io.kyligence.kap.rest.response.LicenseMonitorInfoResponse;
import io.kyligence.kap.rest.response.ProjectCapacityResponse;
import io.kyligence.kap.rest.response.RemoteLicenseResponse;
import io.kyligence.kap.rest.response.ServerInfoResponse;
import lombok.extern.slf4j.Slf4j;
import lombok.val;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.msg.Message;
import org.apache.kylin.common.msg.MsgPicker;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.ShellException;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.model.LicenseInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.kylin.rest.exception.ServerErrorCode.INVALID_LICENSE;

@Slf4j
@Service("licenseInfoService")
public class LicenseInfoService extends BasicService {
    private static final String UNLIMITED = "Unlimited";

    public static final String CODE_WARNING = "001";
    public static final String CODE_ERROR = "002";

    public static final String LICENSE_FILENAME = "LICENSE";
    public static final String HOSTNAME = "hostname";

    private static final Logger logger = LoggerFactory.getLogger(LicenseInfoService.class);

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

    @Autowired
    @Qualifier("normalRestTemplate")
    private RestTemplate restTemplate;

    @Autowired
    private ClusterManager clusterManager;

    @EventListener(AfterMetadataReadyEvent.class)
    public void init() {
        init(code -> log.info("code {}", code));
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
        result.setStatement(System.getProperty(Constants.KE_LICENSE_STATEMENT));
        result.setVersion(System.getProperty(Constants.KE_VERSION));
        result.setDates(System.getProperty(Constants.KE_DATES));
        result.setCommit(System.getProperty(Constants.KE_COMMIT));

        if ("true".equals(System.getProperty(Constants.KE_LICENSE_ISEVALUATION))) {
            result.setEvaluation(true);
        }
        if ("true".equals(System.getProperty(Constants.KE_LICENSE_ISCLOUD))) {
            result.setEvaluation(true);
        }

        if (!StringUtils.isEmpty(System.getProperty(Constants.KE_LICENSE_SERVICEEND))) {
            result.setServiceEnd(System.getProperty(Constants.KE_LICENSE_SERVICEEND));
        } else if (System.getProperty(Constants.KE_DATES) != null && System.getProperty(Constants.KE_DATES).contains(",")) {
            result.setServiceEnd(System.getProperty(Constants.KE_DATES).split(",")[1]);
        }

        result.setNodes(System.getProperty(Constants.KE_LICENSE_NODES));
        result.setVolume(System.getProperty(Constants.KE_LICENSE_VOLUME));
        result.setInfo(System.getProperty(Constants.KE_LICENSE_INFO));
        result.setLevel(System.getProperty(Constants.KE_LICENSE_LEVEL));
        result.setCategory(System.getProperty(Constants.KE_LICENSE_CATEGORY));

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
            setProperty(Constants.KE_METASTORE, prefix, metaStoreId);
        } catch (Exception e) {
            log.error("Cannot get metastore uuid", e);
        }
    }

    private String getMetastoreUUID() {
        ResourceStore store = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv());
        return store.getMetaStoreUUID();
    }

    private String calculateSignature(String input) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            byte[] signature = md.digest(input.getBytes());
            return new String(Base64.encodeBase64(signature));
        } catch (NoSuchAlgorithmException e) {
            return null;
        }
    }

    private String getNetworkAddr() {
        try {
            List<String> result = Lists.newArrayList();
            Enumeration<NetworkInterface> networks = NetworkInterface.getNetworkInterfaces();
            while (networks.hasMoreElements()) {
                NetworkInterface network = networks.nextElement();
                byte[] mac = network.getHardwareAddress();

                StringBuilder sb = new StringBuilder();
                if (mac == null) {
                    continue;
                }
                for (int i = 0; i < mac.length; i++) {
                    sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
                }
                List<String> inetAddrList = Lists.newArrayList();
                for (InterfaceAddress interAddr : network.getInterfaceAddresses()) {
                    inetAddrList.add(interAddr.getAddress().getHostAddress());
                }
                sb.append("(" + org.apache.commons.lang.StringUtils.join(inetAddrList, ",") + ")");
                if (sb.length() > 0) {
                    result.add(sb.toString());
                }
            }
            return org.apache.commons.lang.StringUtils.join(result, ",");
        } catch (Exception e) {
            return org.apache.commons.lang.StringUtils.EMPTY;
        }
    }

    private void gatherVersion(File vfile, UUID prefix) {
        if (vfile.exists()) {
            try (BufferedReader in = new BufferedReader(
                    new InputStreamReader(new FileInputStream(vfile), StandardCharsets.UTF_8))) {
                String line;
                while ((line = in.readLine()) != null) {
                    setProperty(Constants.KE_VERSION, prefix, line);
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
            val r = cmd.execute(HOSTNAME, null);
            if (r.getCode() != 0) {
                log.error("Failed to get hostname, rc=" + r.getCode());
            } else {
                String s = r.getCmd().trim();
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
                    setProperty(Constants.KE_LICENSE_STATEMENT, prefix, statement.toString());

                    String version = in.readLine();
                    setProperty(Constants.KE_LICENSE_VERSION, prefix, version);

                    String dates = in.readLine();
                    setProperty(Constants.KE_DATES, prefix, dates);

                    String license = in.readLine();
                    setProperty(Constants.KE_LICENSE, prefix, license);

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
                    log.info("{}={}", Constants.KE_COMMIT, commit);
                    setProperty(Constants.KE_COMMIT, prefix, commit);
                }
            }
        } catch (IOException ex) {
            log.error("", ex);
        }
    }

    private void gatherStatementInfo(UUID prefix) {
        String statement = System.getProperty(Constants.KE_LICENSE_STATEMENT);

        // set defaults
        setProperty(Constants.KE_LICENSE_ISEVALUATION, prefix, "false");
        setProperty(Constants.KE_LICENSE_CATEGORY, prefix, "4.x");
        setProperty(Constants.KE_LICENSE_LEVEL, prefix, "professional");

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
                    setProperty(Constants.KE_LICENSE_INFO, prefix, line);
                }
                if (line.toLowerCase().contains("evaluation")) {
                    setProperty(Constants.KE_LICENSE_ISEVALUATION, prefix, "true");
                }
                if (line.toLowerCase().contains("for cloud")) {
                    setProperty(Constants.KE_LICENSE_ISCLOUD, prefix, "true");
                }
                extractValue.apply(line, "Service End:").ifPresent(v -> setProperty(Constants.KE_LICENSE_SERVICEEND, prefix, v));
                extractValue.apply(line, "Category:").ifPresent(v -> setProperty(Constants.KE_LICENSE_CATEGORY, prefix, v));
                extractValue.apply(line, "Level:").ifPresent(v -> setProperty(Constants.KE_LICENSE_LEVEL, prefix, v));
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

        setProperty(Constants.KE_LICENSE_VOLUME, prefix, realVolume);
        setProperty(Constants.KE_LICENSE_NODES, prefix, realNode);
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
                    throw new KylinException(INVALID_LICENSE, msg.getLICENSE_INVALID_LICENSE(), CODE_ERROR, e);
                }
            }
            realNode = node;
        }
        return realNode;
    }

    public File backupAndDeleteLicense(String type) throws IOException {
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

    public RemoteLicenseResponse getTrialLicense(LicenseRequest licenseRequest) throws Exception {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        String url = kapConfig.getKyAccountSiteUrl() + "/thirdParty/license";

        LinkedMultiValueMap<String, String> parameters = new LinkedMultiValueMap<String, String>();
        parameters.add("email", licenseRequest.getEmail());
        parameters.add("userName", licenseRequest.getUsername());
        parameters.add("company", licenseRequest.getCompany());
        parameters.add("source", kapConfig.getChannelUser());
        parameters.add("lang", licenseRequest.getLang());
        parameters.add("productType", licenseRequest.getProductType());
        parameters.add("category", licenseRequest.getCategory());

        return restTemplate.postForObject(url, parameters, RemoteLicenseResponse.class);
    }

    public void updateLicense(byte[] bytes) throws IOException {
        clearSystemLicense();
        FileUtils.writeByteArrayToFile(backupAndDeleteLicense("temporary"), bytes);
        gatherLicenseInfo(getDefaultLicenseFile(), getDefaultCommitFile(), getDefaultVersionFile(), null);
        LicenseInfo licenseInfo = extractLicenseInfo();
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        File realLicense = new File(kylinHome, LICENSE_FILENAME);
        File tmpLicense = new File(kylinHome, "LICENSE.temporary");
        try {
            verifyLicense(licenseInfo);
        } catch (Exception e) {
            if (tmpLicense.exists()) {
                FileUtils.copyFile(tmpLicense, realLicense);
                FileUtils.forceDelete(tmpLicense);
                gatherLicenseInfo(getDefaultLicenseFile(), getDefaultCommitFile(), getDefaultVersionFile(), null);
            }
            throw e;
        }
        FileUtils.deleteQuietly(tmpLicense);
        FileUtils.writeByteArrayToFile(backupAndDeleteLicense("backup"), bytes);
        gatherLicenseInfo(getDefaultLicenseFile(), getDefaultCommitFile(), getDefaultVersionFile(), null);
    }

    public void updateLicense(String string) throws IOException {
        updateLicense(string.getBytes("UTF-8"));
    }

    public void clearSystemLicense() {
        System.setProperty(Constants.KE_DATES, "");
        System.setProperty(Constants.KE_LICENSE_LEVEL, "");
        System.setProperty(Constants.KE_LICENSE_CATEGORY, "");
        System.setProperty(Constants.KE_LICENSE_STATEMENT, "");
        System.setProperty(Constants.KE_LICENSE_ISEVALUATION, "");
        System.setProperty(Constants.KE_LICENSE_SERVICEEND, "");
        System.setProperty(Constants.KE_LICENSE_NODES, "");
        System.setProperty(Constants.KE_LICENSE_ISCLOUD, "");
        System.setProperty(Constants.KE_LICENSE_INFO, "");
        System.setProperty(Constants.KE_LICENSE_VERSION, "");
        System.setProperty(Constants.KE_LICENSE_VOLUME, "");
    }

    public boolean filterEmail(String email) {
        String[] emails = { "qq.com", "gmail.com", "sina.com", "163.com", "126.com", "yeah.net", "sohu.com", "tom.com",
                "sogou.com", "139.com", "hotmail.com", "live.com", "live.cn", "live.com.cn", "189.com", "yahoo.com.cn",
                "yahoo.cn", "eyou.com", "21cn.com", "188.com", "foxmail.com" };
        for (String suffix : emails) {
            if (email.endsWith(suffix)) {
                return false;
            }
        }
        Pattern pattern = Pattern.compile("^[a-zA-Z0-9_.-]+@[a-zA-Z0-9-]+(\\.[a-zA-Z0-9-]+)*\\.[a-zA-Z0-9]{2,6}$");
        Matcher matcher = pattern.matcher(email);
        return matcher.find();
    }

    public String requestLicenseInfo() throws IOException {
        LicenseInfo licenseInfo = extractLicenseInfo();
        Map<String, String> systemInfo = Maps.newHashMap();
        systemInfo.put("metastore", getMetastoreUUID());
        systemInfo.put("network", getNetworkAddr());
        systemInfo.put("os.name", System.getProperty("os.name"));
        systemInfo.put("os.arch", System.getProperty("os.arch"));
        systemInfo.put("os.version", System.getProperty("os.version"));
        systemInfo.put("kylin.version", KylinVersion.getCurrentVersion().toString());
        systemInfo.put(HOSTNAME, InetAddress.getLocalHost().getHostName());

        StringBuilder output = new StringBuilder();
        Map<String, String> licenseInfoMap = JsonUtil.convert(licenseInfo, new TypeReference<Map<String, String>>() {
        });
        for (Map.Entry<String, String> entry : licenseInfoMap.entrySet()) {
            output.append(entry.getKey() + ":" + entry.getValue() + "\n");
        }
        for (Map.Entry<String, String> entry : systemInfo.entrySet()) {
            output.append(entry.getKey() + ":" + entry.getValue() + "\n");
        }
        output.append("signature:" + calculateSignature(output.toString()));
        return output.toString();
    }

    public LicenseInfoWithDetailsResponse getLicenseMonitorInfoWithDetail() {
        LicenseInfoWithDetailsResponse licenseInfoWithDetailsResponse = new LicenseInfoWithDetailsResponse();
        SourceUsageRecord latestHistory = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getLatestRecord();
        getLicenseBaseInfo(latestHistory, licenseInfoWithDetailsResponse);

        SourceUsageRecord.ProjectCapacityDetail[] capacityDetails = null;
        if (latestHistory != null) {
            capacityDetails = latestHistory.getCapacityDetails();
        }

        if (capacityDetails != null && capacityDetails.length > 0) {
            CapacityDetailsResponse capacityDetailsResponse;
            List<CapacityDetailsResponse> capacityDetailsResponseList = Lists.newArrayList();
            for (SourceUsageRecord.ProjectCapacityDetail projectCapacityDetail : capacityDetails) {
                capacityDetailsResponse = new CapacityDetailsResponse();
                capacityDetailsResponse.setName(projectCapacityDetail.getName());
                capacityDetailsResponse.setCapacity(projectCapacityDetail.getCapacity());
                capacityDetailsResponse.setStatus(projectCapacityDetail.getStatus());
                capacityDetailsResponseList.add(capacityDetailsResponse);
            }
            licenseInfoWithDetailsResponse.setCapacityDetail(capacityDetailsResponseList);
            licenseInfoWithDetailsResponse.setSize(capacityDetailsResponseList.size());
        }
        return licenseInfoWithDetailsResponse;
    }

    private void getLicenseBaseInfo(SourceUsageRecord latestHistory, LicenseMonitorInfoResponse result) {
        //node part
        int currentNodes = getCurrentNodesNums();
        result.setCurrentNode(currentNodes);
        String serviceNodes = System.getProperty(Constants.KE_LICENSE_NODES);
        if (!StringUtils.isEmpty(serviceNodes) && !UNLIMITED.equals(serviceNodes)) {
            try {
                int maximumNodeNums = Integer.parseInt(serviceNodes);
                result.setNode(maximumNodeNums);
                if (maximumNodeNums < currentNodes) {
                    result.setNodeStatus(SourceUsageRecord.CapacityStatus.OVERCAPACITY);
                }
            } catch (NumberFormatException e) {
                logger.error(
                        "kap.service.nodes occurred java.lang.NumberFormatException: For input string: " + serviceNodes,
                        e);
            }
        }

        //capacity part
        if (latestHistory != null) {
            result.setTime(latestHistory.getCheckTime());
            result.setCurrentCapacity(latestHistory.getCurrentCapacity());
            result.setCapacityStatus(latestHistory.getCapacityStatus());
            result.setCapacity(latestHistory.getLicenseCapacity());
            if (isNotOk(latestHistory.getCapacityStatus())) {
                List<SourceUsageRecord> recentHistories = SourceUsageManager
                        .getInstance(KylinConfig.getInstanceFromEnv()).getLastMonthRecords();
                result.setFirstErrorTime(latestHistory.getCheckTime());
                for (int i = recentHistories.size() - 1; i >= 0; i--) {
                    SourceUsageRecord historyRecord = recentHistories.get(i);
                    if (isNotOk(historyRecord.getCapacityStatus())) {
                        result.setFirstErrorTime(historyRecord.getCheckTime());
                    } else {
                        break;
                    }
                }
            }
        } else {
            result.setCapacityStatus(SourceUsageRecord.CapacityStatus.OVERCAPACITY);
        }

        checkErrorThreshold(result);
    }

    private void checkErrorThreshold(LicenseMonitorInfoResponse licenseMonitorInfoResponse) {
        long firstErrorTime = licenseMonitorInfoResponse.getFirstErrorTime();
        if (firstErrorTime == 0L) {
            return;
        }
        long dayThreshold = (System.currentTimeMillis() - firstErrorTime) / (1000 * 60 * 60 * 24);
        if (dayThreshold >= 30) {
            licenseMonitorInfoResponse.setCapacityStatus(SourceUsageRecord.CapacityStatus.OVERCAPACITY);
        }
    }

    private boolean isNotOk(SourceUsageRecord.CapacityStatus status) {
        return SourceUsageRecord.CapacityStatus.TENTATIVE.equals(status)
                || SourceUsageRecord.CapacityStatus.ERROR.equals(status);
    }

    private int getCurrentNodesNums() {
        //get current nodes
        List<ServerInfoResponse> servers = clusterManager.getQueryServers();
        return CollectionUtils.isEmpty(servers) ? 0 : servers.size();
    }


    public ProjectCapacityResponse getLicenseMonitorInfoByProject(String project, String table) {
        ProjectCapacityResponse projectCapacityResponse = new ProjectCapacityResponse();

        SourceUsageRecord latestHistory = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getLatestRecord();
        SourceUsageRecord.ProjectCapacityDetail projectCapacity = null;
        if (latestHistory != null) {
            projectCapacity = latestHistory.getProjectCapacity(project);
        }

        if (projectCapacity != null) {
            projectCapacityResponse.setName(projectCapacity.getName());
            projectCapacityResponse.setCapacity(projectCapacity.getCapacity());
            projectCapacityResponse.setStatus(projectCapacity.getStatus());

            SourceUsageRecord.TableCapacityDetail[] tables = projectCapacity.getTables();
            if (tables != null && tables.length > 0) {
                CapacityDetailsResponse capacityDetailsResponse;
                List<CapacityDetailsResponse> capacityDetailsResponseList = Lists.newArrayList();
                for (SourceUsageRecord.TableCapacityDetail tableCapacityDetail : tables) {
                    if (table != null && !tableCapacityDetail.getName().toUpperCase().contains(table.toUpperCase())) {
                        continue;
                    }
                    capacityDetailsResponse = new CapacityDetailsResponse();
                    capacityDetailsResponse.setName(tableCapacityDetail.getName());
                    capacityDetailsResponse.setCapacity(tableCapacityDetail.getCapacity());
                    capacityDetailsResponse.setStatus(tableCapacityDetail.getStatus());
                    capacityDetailsResponseList.add(capacityDetailsResponse);
                }
                projectCapacityResponse.setTables(capacityDetailsResponseList);
                projectCapacityResponse.setSize(capacityDetailsResponseList.size());
            }
        }
        return projectCapacityResponse;
    }

    public Map<Long, Long> getLastMonthSourceUsageRecords() {
        Map<Long, Long> records = Maps.newHashMap();
        List<SourceUsageRecord> lastMonthRecords = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getLastMonthRecords();

        if (lastMonthRecords != null) {
            for (SourceUsageRecord record : lastMonthRecords) {
                records.put(record.getCheckTime(), record.getCurrentCapacity());
            }
        }
        return records;
    }

    public TableExtDesc.RowCountStatus refreshTableExtDesc(String project, String table) {
        NTableMetadataManager tableMetadataManager = NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project);
        SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv());
        TableDesc tableDesc = tableMetadataManager.getTableDesc(table);
        TableExtDesc tableExt = tableMetadataManager.getOrCreateTableExt(tableDesc);
        TableExtDesc.RowCountStatus rowCountStatus = tableExt.getRowCountStatus();
        if (rowCountStatus == null || TableExtDesc.RowCountStatus.TENTATIVE.equals(rowCountStatus)) {
            sourceUsageManager.refreshLookupTableRowCount(tableDesc, project);
            sourceUsageManager.updateSourceUsage();
        }
        return tableMetadataManager.getTableExtIfExists(tableDesc).getRowCountStatus() == null
                ? TableExtDesc.RowCountStatus.TENTATIVE
                : tableMetadataManager.getTableExtIfExists(tableDesc).getRowCountStatus();
    }

    public LicenseMonitorInfoResponse getLicenseMonitorInfo() {
        LicenseMonitorInfoResponse licenseMonitorInfoResponse = new LicenseMonitorInfoResponse();
        SourceUsageRecord latestHistory = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv())
                .getLatestRecord();
        getLicenseBaseInfo(latestHistory, licenseMonitorInfoResponse);
        return licenseMonitorInfoResponse;
    }

    public boolean checkIsOverCapacity() {
        LicenseMonitorInfoResponse licenseMonitorInfo = getLicenseMonitorInfo();
        SourceUsageRecord.CapacityStatus capacityStatus = licenseMonitorInfo.getCapacityStatus();
        SourceUsageRecord.CapacityStatus nodeStatus = licenseMonitorInfo.getNodeStatus();
        return SourceUsageRecord.CapacityStatus.OVERCAPACITY.equals(capacityStatus)
                || SourceUsageRecord.CapacityStatus.OVERCAPACITY.equals(nodeStatus);
    }
}
