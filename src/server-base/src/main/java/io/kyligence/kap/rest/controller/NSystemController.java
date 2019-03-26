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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.response.EnvelopeResponse;
import org.apache.kylin.rest.response.ResponseCode;
import org.apache.kylin.rest.service.LicenseInfoService;
import org.apache.kylin.shaded.htrace.org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import io.kyligence.kap.rest.config.initialize.AppInitializedEvent;


@Controller
@Component("nSystemController")
@RequestMapping(value = "/system")
public class NSystemController extends NBasicController {

    private static final Logger logger = LoggerFactory.getLogger(NSystemController.class);


    @Autowired
    private LicenseInfoService licenseInfoService;

    @EventListener(AppInitializedEvent.class)
    public void init() throws IOException {
        if (KylinConfig.getInstanceFromEnv().isDevOrUT()) {
            return;
        }
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        File licenseFile = new File(kylinHome, "LICENSE");
        if (licenseFile.exists()) {
            getLicenseInfo(licenseFile);
        }
    }

    private void getLicenseInfo(File licenseFile) {
        try (BufferedReader in = new BufferedReader(new InputStreamReader(new FileInputStream(licenseFile), "UTF-8"))) {
            String dates;
            while ((dates = in.readLine()) != null) {
                String license = in.readLine();
                if (DigestUtils.md5Hex(dates + DigestUtils.md5Hex(dates)).equals(license)) {
                    System.setProperty("ke.license.valid-dates", dates);
                    System.setProperty("ke.license", license);
                } else {
                    throw new IllegalStateException("license is invalid");
                }
                break;
            }
        } catch (Exception ex) {
            logger.error("license is invalid", ex);
            System.exit(1);
        }
    }

    @RequestMapping(value = "/license", method = {RequestMethod.GET}, produces = {
            "application/vnd.apache.kylin-v2+json"})
    @ResponseBody
    public EnvelopeResponse listLicense() throws IOException {
        return new EnvelopeResponse(ResponseCode.CODE_SUCCESS, licenseInfoService.extractLicenseInfo(), "");
    }
}
