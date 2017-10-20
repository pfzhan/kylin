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

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.protocol.HTTP;
import org.apache.http.util.EntityUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.google.common.collect.Lists;
import com.google.common.io.Files;

@Component("kyBotService")
public class KyBotService extends BasicService {
    public static final String SUCC_CODE = "000";
    public static final String NO_ACCOUNT = "401";
    public static final String AUTH_FAILURE = "402";
    public static final String NO_INTERNET = "403";

    private static final Logger logger = LoggerFactory.getLogger(KyBotService.class);
    private KapConfig kapConfig = KapConfig.getInstanceFromEnv();

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpLocalKyBotPackage(String target, Long startTime, Long endTime, Long currTime, boolean needUpload)
            throws IOException {
        File exportPath = Files.createTempDir();
        if (StringUtils.isEmpty(target)) {
            target = "-all";
        }

        if (needUpload) {
            validateToken();
        }

        ArrayList<String> args = Lists.newArrayList();
        args.add(target);
        args.add(exportPath.getAbsolutePath());
        args.add(Boolean.toString(needUpload));

        if (startTime != null) {
            args.add(Long.toString(startTime));
        }
        if (endTime != null) {
            args.add(Long.toString(endTime));
        }
        if (currTime != null) {
            args.add(Long.toString(currTime));
        }
        runKyBotCLI(args.toArray(new String[0]));
        return getKyBotPackagePath(exportPath);
    }

    protected void runKyBotCLI(String[] args) throws IOException {
        File cwd = new File("");
        logger.debug("Current path: " + cwd.getAbsolutePath());

        logger.debug("KybotClientCLI args: " + Arrays.toString(args));
        File script = new File(KylinConfig.getKylinHome() + File.separator + "bin", "diag.sh");
        if (!script.exists()) {
            throw new RuntimeException("diag.sh not found at " + script.getAbsolutePath());
        }

        String diagCmd = script.getAbsolutePath() + " " + StringUtils.join(args, " ");
        CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        Pair<Integer, String> cmdOutput = executor.execute(diagCmd);

        if (cmdOutput.getKey() != 0) {
            throw new RuntimeException("Failed to generate KyBot package.");
        }
    }

    protected String getKyBotPackagePath(File destDir) {
        File[] files = destDir.listFiles();
        if (files == null) {
            throw new RuntimeException("KyBot package is not available in directory: " + destDir.getAbsolutePath());
        }
        for (File subDir : files) {
            if (subDir.isDirectory()) {
                for (File file : subDir.listFiles()) {
                    if (file.getName().endsWith(".zip")) {
                        return file.getAbsolutePath();
                    }
                }
            }
        }
        throw new RuntimeException("KyBot package not found in directory: " + destDir.getAbsolutePath());
    }

    public String removeLocalKyAccountToken() {
        System.clearProperty("kap.kyaccount.token");
        if (FileUtils.deleteQuietly(getLocalTokenFile())) {
            return SUCC_CODE;
        } else {
            return AUTH_FAILURE;
        }
    }

    private HttpResponse requestKyAccountLogin(String username, String password) {
        DefaultHttpClient client = getHttpClient();
        String url = kapConfig.getKyAccountSSOUrl() + "/uaa/api/tokens";

        // try token
        HttpPost request = new HttpPost(url);

        try {
            if (username != null && password != null) {
                List<NameValuePair> nvps = new ArrayList<>();
                nvps.add(new BasicNameValuePair("username", username));
                nvps.add(new BasicNameValuePair("password", password));
                request.setEntity(new UrlEncodedFormEntity(nvps, HTTP.UTF_8));
            }

            HttpResponse response = client.execute(request);
            return response;
        } catch (Exception e) {
            logger.error("Authentication failed due to exception.", e.getLocalizedMessage());
            return null;
        } finally {
            request.releaseConnection();
        }
    }

    public boolean checkInternetAccess() {
        HttpResponse response = requestKyAccountLogin(null, null);
        if (response != null) {
            return true;
        } else {
            return false;
        }
    }

    public String fetchAndSaveKyAccountToken(String username, String password) {
        HttpResponse response = requestKyAccountLogin(username, password);
        if (response != null) {
            if (response.getStatusLine().getStatusCode() == 200) {
                try {
                    String token = IOUtils.toString(response.getEntity().getContent());
                    if (!StringUtils.isEmpty(token)) {
                        File localTokenFile = getLocalTokenFile();
                        FileUtils.write(localTokenFile, token);

                        readLocalKyAccountToken();

                        return SUCC_CODE;
                    }
                } catch (Exception e) {
                    logger.error("Failed to persist kyaccount token. ", e);
                }
            }
        }
        return AUTH_FAILURE;
    }

    public String fetchKyAccountDetail() {
        String token = getKyAccountToken();

        DefaultHttpClient client = getHttpClient();
        String url = kapConfig.getKyBotSiteUrl() + "/api/user/authentication";
        HttpGet request = new HttpGet(url);

        try {
            request.setHeader("authorization", "bearer " + token);
            HttpResponse response = client.execute(request);
            if (response.getStatusLine().getStatusCode() == 200) {
                Map<String, Object> userDetailMap = JsonUtil.readValue(EntityUtils.toString(response.getEntity()),
                        Map.class);
                Map<String, Object> userDetails = (Map<String, Object>) userDetailMap.get("userDetails");
                if (userDetails != null) {
                    return (String) userDetails.get("username");
                }
            }
        } catch (Exception ex) {
            logger.error("Failed to get user detail result from kybot.", ex);
        } finally {
            request.releaseConnection();
        }
        return null;
    }

    private File getLocalTokenFile() {
        return new File(KylinConfig.getKylinHome(), ".kyaccount");
    }

    public void readLocalKyAccountToken() {
        if (kapConfig.getKyAccountToken() == null) {
            try {
                File tokenFile = getLocalTokenFile();
                if (tokenFile.exists()) {
                    String token = FileUtils.readFileToString(tokenFile);
                    System.setProperty("kap.kyaccount.token", token);
                }
            } catch (IOException e) {
                logger.error("Failed to read kyaccount token.", e);
            }
        }
    }

    private DefaultHttpClient getHttpClient() {
        String proxyServer = kapConfig.getHttpProxyHost();
        int proxyPort = kapConfig.getHttpProxyPort();

        DefaultHttpClient client = new DefaultHttpClient();
        if (proxyServer != null && proxyPort > 0) {
            HttpHost proxy = new HttpHost(proxyServer, proxyPort);
            client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
        }

        return client;
    }

    public String validateToken() {
        String token = getKyAccountToken();
        if (StringUtils.isEmpty(token)) {
            return NO_ACCOUNT;
        }

        DefaultHttpClient client = getHttpClient();
        String url = kapConfig.getKyBotSiteUrl() + "/api/user/authentication";

        // try token
        HttpPost request = new HttpPost(url);

        try {
            request.setHeader("authorization", "bearer " + token);
            HttpResponse response = client.execute(request);
            return response.getStatusLine().getStatusCode() == 200 ? SUCC_CODE : AUTH_FAILURE;
        } catch (Exception ex) {
            logger.error("Authentication failed due to exception.", ex);
            return AUTH_FAILURE;
        } finally {
            request.releaseConnection();
        }
    }

    public String setKybotAgreement() {
        String token = getKyAccountToken();
        if (StringUtils.isEmpty(token)) {
            return NO_ACCOUNT;
        }

        DefaultHttpClient client = getHttpClient();
        String url = kapConfig.getKyBotSiteUrl() + "/api/user/agreement";
        HttpPost request = new HttpPost(url);

        try {
            request.setHeader("authorization", "bearer " + token);
            HttpResponse response = client.execute(request);
            return EntityUtils.toString(response.getEntity());
        } catch (Exception ex) {
            logger.error("Failed to set user agreement result from kybot.", ex);
            return AUTH_FAILURE;
        } finally {
            request.releaseConnection();
        }
    }

    public boolean getKybotAgreement() {
        String token = getKyAccountToken();

        DefaultHttpClient client = getHttpClient();
        String url = kapConfig.getKyBotSiteUrl() + "/api/user/agreement";
        HttpGet request = new HttpGet(url);

        try {
            request.setHeader("authorization", "bearer " + token);
            HttpResponse response = client.execute(request);
            if (response.getStatusLine().getStatusCode() == 200) {
                Map<String, Object> agreementMap = JsonUtil.readValue(EntityUtils.toString(response.getEntity()),
                        Map.class);
                return (boolean) agreementMap.get("isUserAgreement");
            }
        } catch (Exception ex) {
            logger.error("Failed to get user agreement result from kybot.", ex);
        } finally {
            request.releaseConnection();
        }

        return false;
    }

    public boolean getDaemonStatus() {
        File script = FileUtils.getFile(KylinConfig.getKylinHome(), "kybot", "agent", "bin", "status.sh");
        if (!script.exists()) {
            throw new RuntimeException("kybot/agent/bin/status.sh not found at " + script.getAbsolutePath());
        }
        try {
            CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
            Pair<Integer, String> cmdOutput = executor.execute(script.getAbsolutePath());
            return cmdOutput.getValue().contains("running: true");
        } catch (IOException e) {
            logger.error("Failed to execute kybot/agent/bin/status.sh");
            return false;
        }
    }

    public boolean startDaemon() {
        File script = FileUtils.getFile(KylinConfig.getKylinHome(), "kybot", "agent", "bin", "enable.sh");
        if (!script.exists()) {
            throw new RuntimeException("kybot/agent/bin/enable.sh not found at " + script.getAbsolutePath());
        }
        try {
            Runtime.getRuntime().exec(script.getAbsolutePath());
            Thread.sleep(3000L); // Sleep 3s to wait for agent start
            return getDaemonStatus();
        } catch (IOException | InterruptedException e) {
            logger.error("Failed to execute kybot/agent/bin/enable.sh");
            return false;
        }
    }

    public boolean stopDaemon() {
        File script = FileUtils.getFile(KylinConfig.getKylinHome(), "kybot", "agent", "bin", "disable.sh");
        if (!script.exists()) {
            throw new RuntimeException("kybot/agent/bin/disable.sh not found at " + script.getAbsolutePath());
        }
        try {
            CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
            Pair<Integer, String> cmdOutput = executor.execute(script.getAbsolutePath());
            return cmdOutput.getKey() == 0;
        } catch (IOException e) {
            logger.error("Failed to execute kybot/agent/bin/disable.sh");
            return false;
        }
    }

    private String getKyAccountToken() {
        readLocalKyAccountToken();
        return kapConfig.getKyAccountToken();
    }
}