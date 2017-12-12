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

import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.msg.KapMessage;
import io.kyligence.kap.rest.msg.KapMsgPicker;
import io.kyligence.kap.rest.request.KyAccountLoginRequest;
import io.kyligence.kap.rest.service.KyBotService;

@Controller
public class KyBotController extends BasicController {
    @Autowired
    @Qualifier("kyBotService")
    private KyBotService kybotService;

    @RequestMapping(value = "/kybot/dump", method = { RequestMethod.GET }, produces = { "application/json",
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public void localDumpKyBotPackage(@RequestParam(value = "startTime", required = false) Long startTime,
            @RequestParam(value = "endTime", required = false) Long endTime,
            @RequestParam(value = "currentTime", required = false) Long currTime,
            @RequestParam(value = "types[]", required = false) String[] types,
            @RequestParam(value = "target", required = false) String target, final HttpServletRequest request,
            final HttpServletResponse response) {
        KapMessage msg = KapMsgPicker.getMsg();

        String filePath;
        try {
            filePath = kybotService.dumpLocalKyBotPackage(target, startTime, endTime, currTime, false, types);
        } catch (IOException e) {
            throw new InternalErrorException(msg.getDUMP_KYBOT_PACKAGE_FAIL());
        }

        setDownloadResponse(filePath, response);
    }

    @RequestMapping(value = "/kybot/upload", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse uploadToKybot(@RequestParam(value = "startTime", required = false) Long startTime,
            @RequestParam(value = "endTime", required = false) Long endTime,
            @RequestParam(value = "currentTime", required = false) Long currTime,
            @RequestParam(value = "types[]", required = false) String[] types,
            @RequestParam(value = "target", required = false) String target) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        String path = kybotService.dumpLocalKyBotPackage(target, startTime, endTime, currTime, true, types);
        boolean retVal = !StringUtils.isEmpty(path);

        return new EnvelopeResponse(retVal ? KyBotService.SUCC_CODE : KyBotService.AUTH_FAILURE, retVal,
                msg.getKYACCOUNT_AUTH_FAILURE());
    }

    @RequestMapping(value = "/kyaccount/auth", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse checkKyaccountAuth(@RequestBody KyAccountLoginRequest request) throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        boolean internetAccess = kybotService.checkInternetAccess();
        if (!internetAccess) {
            return new EnvelopeResponse(KyBotService.NO_INTERNET, false, msg.getKYBOT_NOACCESS());
        }

        String retCode = kybotService.fetchAndSaveKyAccountToken(request.getUsername(), request.getPassword());
        if (retCode.equals(kybotService.SUCC_CODE)) {
            return new EnvelopeResponse(retCode, true, null);
        } else {
            return new EnvelopeResponse(retCode, false, msg.getKYACCOUNT_AUTH_FAILURE());
        }
    }

    @RequestMapping(value = "/kyaccount/logout", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse logoutKyAccount() throws IOException {
        KapMessage msg = KapMsgPicker.getMsg();

        if (kybotService.getDaemonStatus()) {
            kybotService.stopDaemon();
        }
        String retCode = kybotService.removeLocalKyAccountToken();

        if (retCode.equals(kybotService.SUCC_CODE)) {
            return new EnvelopeResponse(retCode, true, null);
        } else {
            return new EnvelopeResponse(retCode, false, msg.getKYACCOUNT_AUTH_FAILURE());
        }
    }

    @RequestMapping(value = "/kyaccount/current", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getKyAccountDetail() throws IOException {
        String userName = kybotService.fetchKyAccountDetail();
        return new EnvelopeResponse(KyBotService.SUCC_CODE, userName, null);
    }

    @RequestMapping(value = "/kyaccount", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getKyaccountToken() {
        KapMessage msg = KapMsgPicker.getMsg();

        kybotService.readLocalKyAccountToken();
        String token = KapConfig.getInstanceFromEnv().getKyAccountToken();
        return new EnvelopeResponse(KyBotService.SUCC_CODE, token, null);
    }

    @RequestMapping(value = "/kybot/agreement", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getKybotAgreement() {
        String ret = kybotService.validateToken();
        if (KyBotService.AUTH_FAILURE.equals(ret) || KyBotService.NO_ACCOUNT.equals(ret)) {
            return new EnvelopeResponse(ret, null, null);
        }
        return new EnvelopeResponse(KyBotService.SUCC_CODE, kybotService.getKybotAgreement(), null);
    }

    @RequestMapping(value = "/kybot/agreement", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse setKybotAgreement() {
        String ret = kybotService.setKybotAgreement();
        if (KyBotService.AUTH_FAILURE.equals(ret) || KyBotService.NO_ACCOUNT.equals(ret)) {
            return new EnvelopeResponse(ret, null, null);
        }
        return new EnvelopeResponse(KyBotService.SUCC_CODE, ret, null);
    }

    @RequestMapping(value = "/kybot/daemon/status", method = { RequestMethod.GET }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getKyBotDaemonStatus() {
        KapMessage msg = KapMsgPicker.getMsg();

        boolean isRunning = kybotService.getDaemonStatus();
        return new EnvelopeResponse(KyBotService.SUCC_CODE, isRunning, null);
    }

    @RequestMapping(value = "/kybot/daemon/start", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse startKyBotDaemon() {
        KapMessage msg = KapMsgPicker.getMsg();

        boolean started = kybotService.startDaemon();
        return new EnvelopeResponse(KyBotService.SUCC_CODE, started, null);
    }

    @RequestMapping(value = "/kybot/daemon/stop", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse stopKyBotDaemon() {
        KapMessage msg = KapMsgPicker.getMsg();

        boolean stopped = kybotService.stopDaemon();
        return new EnvelopeResponse(KyBotService.SUCC_CODE, stopped, null);
    }

    @RequestMapping(value = "/kybot/servers", method = { RequestMethod.POST }, produces = {
            "application/vnd.apache.kylin-v2+json" })
    @ResponseBody
    public EnvelopeResponse getServerList() {
        String[] servers = kybotService.getServerList();

        return new EnvelopeResponse(KyBotService.SUCC_CODE, servers, null);
    }
}
