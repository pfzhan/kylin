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

package io.kyligence.kap.rest.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.InetAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.URLEncoder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.KylinVersion;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import io.kyligence.kap.rest.client.HttpClient;
import io.kyligence.kap.rest.request.LicenseRequest;
import io.kyligence.kap.rest.response.RemoteLicenseResponse;

@Component("licenseInfoService")
public class LicenseInfoService extends BasicService {
    private static final Logger logger = LoggerFactory.getLogger(LicenseInfoService.class);
    private KapConfig kapConfig = KapConfig.getInstanceFromEnv();

    public Map<String, String> extractLicenseInfo() {
        Map<String, String> result = new HashMap<>();
        String lic = System.getProperty("kap.license.statement");
        result.put("kap.license.statement", lic);
        result.put("kap.version", System.getProperty("kap.version"));
        result.put("kap.dates", System.getProperty("kap.dates"));
        result.put("kap.commit", System.getProperty("kap.commit"));
        result.put("kylin.commit", System.getProperty("kylin.commit"));

        try {
            if (lic != null) {
                BufferedReader reader = new BufferedReader(new StringReader(lic));
                String line;
                while ((line = reader.readLine()) != null) {
                    if (line.toLowerCase().contains("evaluation")) {
                        result.put("kap.license.isEvaluation", "true");
                    }
                    if (line.startsWith("Service End:")) {
                        result.put("kap.license.serviceEnd", line.substring("Service End:".length()).trim());
                    }
                }
                reader.close();
            }

        } catch (IOException e) {
            // ignore
        }

        return result;
    }

    public String requestLicenseInfo() throws IOException {
        Map<String, String> currentLicenseInfo = extractLicenseInfo();
        Map<String, String> systemInfo = Maps.newHashMap();
        systemInfo.put("metastore", getMetastoreUUID());
        systemInfo.put("network", getNetworkAddr());
        systemInfo.put("os.name", System.getProperty("os.name"));
        systemInfo.put("os.arch", System.getProperty("os.arch"));
        systemInfo.put("os.version", System.getProperty("os.version"));
        systemInfo.put("kylin.version", KylinVersion.getCurrentVersion().toString());
        systemInfo.put("hostname", InetAddress.getLocalHost().getHostName());

        StringBuilder output = new StringBuilder();
        for (Map.Entry<String, String> entry : currentLicenseInfo.entrySet()) {
            output.append(entry.getKey() + ":" + entry.getValue() + "\n");
        }
        for (Map.Entry<String, String> entry : systemInfo.entrySet()) {
            output.append(entry.getKey() + ":" + entry.getValue() + "\n");
        }
        output.append("signature:" + calculateSignature(output.toString()));
        return output.toString();
    }

    private String getMetastoreUUID() throws IOException {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        ResourceStore store = ResourceStore.getStore(kylinConfig);
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
                if (mac != null) {
                    for (int i = 0; i < mac.length; i++) {
                        sb.append(String.format("%02X%s", mac[i], (i < mac.length - 1) ? "-" : ""));
                    }
                    List<String> inetAddrList = Lists.newArrayList();
                    for (InterfaceAddress interAddr : network.getInterfaceAddresses()) {
                        inetAddrList.add(interAddr.getAddress().getHostAddress());
                    }
                    sb.append("(" + StringUtils.join(inetAddrList, ",") + ")");
                }
                if (sb.length() > 0) {
                    result.add(sb.toString());
                }
            }
            return StringUtils.join(result, ",");
        } catch (Exception e) {
            return StringUtils.EMPTY;
        }
    }

    public RemoteLicenseResponse getTrialLicense(LicenseRequest licenseRequest) {
        String proxyServer = kapConfig.getHttpProxyHost();
        int proxyPort = kapConfig.getHttpProxyPort();
        String url;
        try {
            url = kapConfig.getKyAccountSiteUrl()
                    + String.format("/thirdParty/license?userName=%s&email=%s&company=%s&lang=%s",
                            URLEncoder.encode(licenseRequest.getUserName(), "UTF-8"),
                            URLEncoder.encode(licenseRequest.getEmail(), "UTF-8"),
                            URLEncoder.encode(licenseRequest.getCompany(), "UTF-8"), licenseRequest.getLang());
        } catch (UnsupportedEncodingException e) {
            url = kapConfig.getKyAccountSiteUrl() + String.format(
                    "/thirdParty/license?userName=%s&email=%s&company=%s&lang=%s", licenseRequest.getUserName(),
                    licenseRequest.getEmail(), licenseRequest.getCompany(), licenseRequest.getLang());
            logger.error("URLDecoder decode url error, url=" + url, e);
        }
        String response = HttpClient.doGet(url, proxyServer, proxyPort);
        if (response == null)
            return null;
        try {
            return JsonUtil.readValue(response, RemoteLicenseResponse.class);
        } catch (IOException e) {
            logger.error("from json to RemoteLicenseResponse error", e);
        }
        return null;
    }
}
