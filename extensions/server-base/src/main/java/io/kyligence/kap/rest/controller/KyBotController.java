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
import org.apache.kylin.rest.controller.BasicController;
import org.apache.kylin.rest.exception.InternalErrorException;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.service.KyBotService;

@Controller
public class KyBotController extends BasicController {
    @Autowired
    @Qualifier("kyBotService")
    private KyBotService kybotService;

    @RequestMapping(value = "/kybot/dump", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public void localDumpKyBotPackage(final HttpServletRequest request, final HttpServletResponse response) {
        String filePath;
        try {
            filePath = kybotService.dumpLocalKyBotPackage(false);
        } catch (IOException e) {
            throw new InternalErrorException("Failed to dump kybot package. " + e.getMessage(), e);
        }

        setDownloadResponse(filePath, response);
    }

    @RequestMapping(value = "/kybot/upload", method = { RequestMethod.GET }, produces = { "application/json" })
    @ResponseBody
    public EnvelopeResponse uploadToKybot(final HttpServletResponse response) {
        try {
            String retCode = kybotService.checkServiceConnection();
            boolean retVal = false;
            if (retCode.equals(KyBotService.SUCC_CODE)) {
                String path = kybotService.dumpLocalKyBotPackage(true);
                retVal = !StringUtils.isEmpty(path);
            } else {
                response.setStatus(HttpServletResponse.SC_BAD_REQUEST);
            }
            return new EnvelopeResponse(retCode, retVal, null);
        } catch (IOException e) {
            throw new InternalErrorException("Failed to dump kybot package. ", e);
        }
    }
}
