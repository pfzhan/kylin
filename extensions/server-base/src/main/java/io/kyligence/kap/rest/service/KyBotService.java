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
import java.nio.charset.Charset;
import java.util.Arrays;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpHost;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.CliCommandExecutor;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.exception.BadRequestException;
import org.apache.kylin.rest.security.PasswordPlaceholderConfigurer;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.google.common.io.Files;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;

@Component("kyBotService")
public class KyBotService extends BasicService {
    public static final String SUCC_CODE = "000";
    public static final String NO_ACCOUNT = "401";
    public static final String AUTH_FAILURE = "402";

    private static final Logger logger = LoggerFactory.getLogger(KyBotService.class);

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public String dumpLocalKyBotPackage(boolean needUpload) throws IOException {
        File exportPath = Files.createTempDir();
        String[] args = { "-all", exportPath.getAbsolutePath(), Boolean.toString(needUpload) };
        runKyBotCLI(args);
        return getKyBotPackagePath(exportPath);
    }

    protected void runKyBotCLI(String[] args) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        File cwd = new File("");
        logger.debug("Current path: " + cwd.getAbsolutePath());

        logger.debug("KybotClientCLI args: " + Arrays.toString(args));
        File script = new File(KylinConfig.getKylinHome() + File.separator + "bin", "diag.sh");
        if (!script.exists()) {
            throw new BadRequestException(String.format(msg.getDIAG_NOT_FOUND(), script.getAbsolutePath()));
        }

        String diagCmd = script.getAbsolutePath() + " " + StringUtils.join(args, " ");
        CliCommandExecutor executor = KylinConfig.getInstanceFromEnv().getCliCommandExecutor();
        Pair<Integer, String> cmdOutput = executor.execute(diagCmd);

        logger.debug("Cmdoutput: " + cmdOutput.getKey());
        if (cmdOutput.getKey() != 0) {
            throw new BadRequestException(msg.getGENERATE_KYBOT_PACKAGE_FAIL());
        }
    }

    protected String getKyBotPackagePath(File destDir) {
        KapMessage msg = KapMsgPicker.getMsg();

        File[] files = destDir.listFiles();
        if (files == null) {
            throw new BadRequestException(String.format(msg.getKYBOT_PACKAGE_NOT_AVAILABLE(), destDir.getAbsolutePath()));
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
        throw new BadRequestException(String.format(msg.getKYBOT_PACKAGE_NOT_FOUND(), destDir.getAbsolutePath()));
    }

    public String checkServiceConnection() {
        KapConfig kapConfig = KapConfig.getInstanceFromEnv();
        String username = kapConfig.getKyAccountUsename();
        String password = kapConfig.getKyAccountPassword();
        String token = kapConfig.getKyAccountToken();

        if ((StringUtils.isEmpty(username) || StringUtils.isEmpty(password)) && StringUtils.isEmpty(token)) {
            return NO_ACCOUNT;
        }

        String proxyServer = kapConfig.getHttpProxyHost();
        int proxyPort = kapConfig.getHttpProxyPort();

        DefaultHttpClient client = new DefaultHttpClient();
        if (proxyServer != null && proxyPort > 0) {
            HttpHost proxy = new HttpHost(proxyServer, proxyPort);
            client.getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
        }

        String url = kapConfig.getKyBotSiteUrl() + "/api/user/authentication";

        boolean isSucc = false;

        // try username & password firstly
        if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
            password = PasswordPlaceholderConfigurer.decrypt(password);
            byte[] encodedAuth = Base64.encodeBase64((username + ":" + password).getBytes(Charset.forName("ISO-8859-1")));
            String authHeader = "Basic " + new String(encodedAuth);
            isSucc = testServerConnection(client, url, authHeader);
            logger.debug("Check authentication with username: URL={}, ProxyHost={}, ProxyPort={}, Username={}", url, proxyServer, proxyPort, username);
        }

        // try token
        if (!StringUtils.isEmpty(token) && !isSucc) {
            String authHeader = "bearer " + token;
            isSucc = testServerConnection(client, url, authHeader);
            logger.debug("Check authentication with token: URL={}, ProxyHost={}, ProxyPort={}, Token={}", url, proxyServer, proxyPort, token);
        }

        return isSucc ? SUCC_CODE : AUTH_FAILURE;
    }

    private boolean testServerConnection(DefaultHttpClient client, String url, String authHeader) {
        HttpPost request = new HttpPost(url);

        try {
            request.setHeader("authorization", authHeader);
            HttpResponse response = client.execute(request);
            return response.getStatusLine().getStatusCode() == 200;
        } catch (Exception ex) {
            logger.error("Authentication failed due to exception: " + ex.getMessage());
            return false;
        } finally {
            request.releaseConnection();
        }
    }
}
