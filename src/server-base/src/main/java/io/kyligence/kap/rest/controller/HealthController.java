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

package io.kyligence.kap.rest.controller;

import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_JSON;
import static io.kyligence.kap.common.constant.HttpConstant.HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import javax.crypto.SecretKey;
import javax.servlet.http.HttpServletRequest;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.query.util.QueryLimiter;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.google.common.collect.Lists;

import io.kyligence.kap.common.util.SecretKeyUtil;
import io.kyligence.kap.rest.response.HealthResponse;
import io.kyligence.kap.rest.service.HealthService;
import io.kyligence.kap.tool.daemon.ServiceOpLevelEnum;
import io.kyligence.kap.tool.daemon.checker.KEStatusChecker;
import io.kyligence.kap.tool.util.ToolUtil;
import io.swagger.annotations.ApiOperation;
import lombok.Getter;
import lombok.extern.log4j.Log4j;

@Log4j
@Controller
@RequestMapping(value = "/api/health", produces = { HTTP_VND_APACHE_KYLIN_JSON, HTTP_VND_APACHE_KYLIN_V4_PUBLIC_JSON })
public class HealthController extends NBasicController {

    private static final int MAX_TOKEN_LENGTH = 64;

    private static final long VALIDATION_DURATION = 20L * 1000;

    @Getter
    private static String KE_PID;

    public synchronized void setKePid(String pid) {
        KE_PID = pid;
    }

    @Autowired
    @Qualifier("healthService")
    private HealthService healthService;

    @ApiOperation(value = "health APIs", tags = {"SM"})
    @PostMapping(value = "/instance_info")
    @ResponseBody
    public EnvelopeResponse<HealthResponse> getHealthStatus(HttpServletRequest request) throws IOException {
        try (BufferedInputStream bis = new BufferedInputStream(request.getInputStream())) {
            byte[] encryptedToken = readEncryptedToken(bis);
            if (!validateToken(encryptedToken)) {
                return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, null, KEStatusChecker.PERMISSION_DENIED);
            }
            return getHealthStatus();
        }
    }

    @ApiOperation(value = "health APIs", tags = {"SM"})
    @PostMapping(value = "/instance_service/{state}")
    @ResponseBody
    public EnvelopeResponse<String> changeServerState(HttpServletRequest request, @PathVariable String state)
            throws IOException {
        try (BufferedInputStream bis = new BufferedInputStream(request.getInputStream())) {
            byte[] encryptedToken = readEncryptedToken(bis);
            if (!validateToken(encryptedToken)) {
                return new EnvelopeResponse<>(KylinException.CODE_UNDEFINED, "", KEStatusChecker.PERMISSION_DENIED);
            }

            if (ServiceOpLevelEnum.QUERY_DOWN_GRADE.getOpType().equals(state)) {
                QueryLimiter.downgrade();
            } else if (ServiceOpLevelEnum.QUERY_UP_GRADE.getOpType().equals(state)) {
                QueryLimiter.recover();
            } else {
                throw new IllegalArgumentException("Illegal server state: " + state);
            }

            return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, "", "");
        }
    }

    public EnvelopeResponse<HealthResponse> getHealthStatus() {
        HealthResponse.RestartSparkStatusResponse sparkRestartStatus = healthService.getRestartSparkStatus();
        List<HealthResponse.CanceledSlowQueryStatusResponse> canceledSlowQueriesStatus = healthService
                .getCanceledSlowQueriesStatus();
        HealthResponse healthResponse = new HealthResponse(sparkRestartStatus, canceledSlowQueriesStatus);
        return new EnvelopeResponse<>(KylinException.CODE_SUCCESS, healthResponse, "");
    }

    private byte[] readEncryptedToken(BufferedInputStream bis) throws IOException {
        ArrayList<Byte> byteList = Lists.newArrayList();
        int data;
        int readByteCount = 0;
        while ((data = bis.read()) != -1) {
            byteList.add((byte) data);
            if (++readByteCount > MAX_TOKEN_LENGTH) {
                return null;
            }
        }
        byte[] encryptedToken = new byte[readByteCount];
        for (int i = 0; i < readByteCount; i++) {
            encryptedToken[i] = byteList.get(i);
        }
        return encryptedToken;
    }

    private boolean validateToken(byte[] encryptedToken) {
        if (null == encryptedToken || encryptedToken.length == 0) {
            return false;
        }
        try {
            SecretKey secretKey = SecretKeyUtil.getKGSecretKey();
            String originalToken = SecretKeyUtil.decryptToken(secretKey, encryptedToken);
            String[] parts = originalToken.split("_");
            if (parts.length != 2) {
                return false;
            }

            if (null == getKE_PID()) {
                setKePid(ToolUtil.getKylinPid());
            }

            if (!parts[0].equals(getKE_PID())) {
                return false;
            }
            long timestamp = Long.parseLong(parts[1]);
            return System.currentTimeMillis() - timestamp <= VALIDATION_DURATION;
        } catch (Exception e) {
            log.error("Validate token failed! ", e);
            return false;
        }
    }
}
