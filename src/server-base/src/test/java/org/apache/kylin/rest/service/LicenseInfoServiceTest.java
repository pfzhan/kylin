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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.rest.service;

import java.io.File;
import java.io.IOException;

import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KapConfig;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.test.util.ReflectionTestUtils;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.web.client.RestTemplate;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.request.LicenseRequest;
import io.kyligence.kap.rest.response.RemoteLicenseResponse;
import io.kyligence.kap.rest.rules.ClearKEPropertiesRule;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class LicenseInfoServiceTest extends NLocalFileMetadataTestCase {

    @Rule
    public ClearKEPropertiesRule clearKEProperties = new ClearKEPropertiesRule();

    @Mock
    private RestTemplate restTemplate = Mockito.spy(new RestTemplate());

    @InjectMocks
    private LicenseInfoService licenseInfoService = Mockito.spy(new LicenseInfoService());

    @Before
    public void setupResource() {
        createTestMetadata();
        ReflectionTestUtils.setField(licenseInfoService, "restTemplate", restTemplate);
        val commitFile = LicenseInfoService.getDefaultCommitFile();
        try {
            FileUtils.write(commitFile,
                    "daa973eada22ab76b7a740e1d81d5ef903809ace@KAP\n" + "Build with MANUAL at 2019-07-05 10:12:34");
            FileUtils.write(LicenseInfoService.getDefaultVersionFile(), "Kyligence Enterprise 4.0.0-SNAPSHOT");
        } catch (IOException ignore) {
        }
    }

    @After
    public void tearDown() {
        FileUtils.deleteQuietly(LicenseInfoService.getDefaultVersionFile());
        FileUtils.deleteQuietly(LicenseInfoService.getDefaultCommitFile());
        cleanupTestMetadata();
    }

    @Test
    public void testBasics() throws IOException {
        getTestConfig().setProperty("kylin.env", "PROD");
        licenseInfoService.init();
        Assert.assertEquals("2019-06-01,2019-07-30", System.getProperty(LicenseInfoService.KE_DATES));
        Assert.assertEquals("professional", System.getProperty(LicenseInfoService.KE_LICENSE_LEVEL));
        Assert.assertEquals(
                "19d4801b6dardchr83bp3i7wadbdvycs8ay7ibicu2msfogl6kiwz7z3dmdizepmicl3bgqznn34794jt5g51sutofcfpn9jeiw5k3cvt2750faxw7ip1fp08mt3og6xijt4x02euf1zkrn5m7huwal8lqms3gmn0d5i8y2dqlvkvpqtwz3m9tqcnq6n4lznthbdtfncdqsly7a8v9pndh1cav2tdcczzs17ns6e0d4izeatwybr25lir5f5s6qe4ry10x2fkqco7unb4h4ivx8jo6vdb5sp3r4738zhlvrbdwfa38s3wh82lrnugrhxq8eap3rebq9dz8xka713aui4v2acquulicdadt63cv0biz7y7eccfh1tri60526b2bmon71k29n6p29tsbhyl2wdx5hsjuxg2wd993hcndot1fc5oz8kebopqrudyf4o7tjc5ca0bvtysnw3gn64c1sd2iw2rlhlxk7c5szp6kde8dvitteoqo1oufum5eyjbk1q2fegf9vpyng3bs6c6qfoibc2wvxgjn4hnismbsr4ovwe5gvam74ikdromn8dxv91e5wuvcqml92jgfoj4g0xzrns05hsqs55a5a9ao44f6m2eccscq4crfm5dxwdl7xbmmmj1yfgpygco4mvh9ksitsxoy30v6dgse76wmyemjymyaa2f6my83vu55z9vhywv6a4har3tep32dg3mvol1arsia8bllis4awfqjpw57lpv1fmt5n8ns8vqvle09cpehrlkt5kjcaucwb64c25q8zvikgtm2p0ywfnsapm97fxloymcqp0vgwmqzt3feaq8o6mzjaqmgap7r7gtn1k1awwxjs1sd91g4y1emab14hs",
                System.getProperty(LicenseInfoService.KE_LICENSE));
        getTestConfig().setProperty("kylin.env", "UT");

    }

    @Test
    public void testParseFailed() throws IOException {
        val licenseFilePath = LicenseInfoService.getDefaultLicenseFile().getAbsolutePath();
        try {
            FileUtils.copyFile(new File(licenseFilePath), new File(licenseFilePath + ".backup"));
            FileUtils.copyFile(new File("src/test/resources/ut_license/wrong_volume_license"),
                    new File(licenseFilePath));
            licenseInfoService.init(code -> log.info("code {}", code));
            Assert.assertTrue(new File(licenseFilePath + ".error").exists());
        } finally {
            FileUtils.copyFile(new File(licenseFilePath + ".backup"), new File(licenseFilePath));
            FileUtils.deleteQuietly(new File(licenseFilePath + ".error"));
            FileUtils.deleteQuietly(new File(licenseFilePath + ".backup"));
        }
    }

    @Test
    public void testGetTrialLicense() throws Exception {
        LicenseRequest licenseRequest = new LicenseRequest();
        licenseRequest.setUsername("a");
        licenseRequest.setCompany("b");
        licenseRequest.setEmail("c");
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
        RemoteLicenseResponse response = new RemoteLicenseResponse();
        response.setSuccess(true);
        Mockito.when(restTemplate.postForObject(url, parameters, RemoteLicenseResponse.class)).thenReturn(response);

        RemoteLicenseResponse remoteLicenseResponse = licenseInfoService.getTrialLicense(licenseRequest);
        Assert.assertEquals(remoteLicenseResponse, response);

    }

    @Test
    public void testUpdateLicense() throws Exception {
        String license = "wu xiao xu ke zheng";
        licenseInfoService.updateLicense(license);
        File kylinHome = KapConfig.getKylinHomeAtBestEffort();
        File realLicense = new File(kylinHome, "LICENSE");
        String fileLicense = FileUtils.readFileToString(realLicense);
        // This is also success because NOT APPLY LICENSE PATCH
        assert fileLicense.equals(license);

        license = "Evaluation license for Kyligence Enterprise\n" + "Category: 4.x\n" + "SLA Service: NO\n"
                + "Volume: 1\n" + "Level: professional\n"
                + "Insight License: 5 users; evaluation; 2019-06-01,2019-07-30\n" + "====\n" + "Kyligence Enterprise\n"
                + "2019-06-01,2019-07-30\n" + "19d4801b6dardchr83bp3i7wadbdvycs8ay7ibicu2msfogl6kiwz7z3"
                + "dmdizepmicl3bgqznn34794jt5g51sutofcfpn9jeiw5k3cvt2750faxw7"
                + "ip1fp08mt3og6xijt4x02euf1zkrn5m7huwal8lqms3gmn0d5i8y2dqlvkv"
                + "pqtwz3m9tqcnq6n4lznthbdtfncdqsly7a8v9pndh1cav2tdcczzs17ns6e0"
                + "d4izeatwybr25lir5f5s6qe4ry10x2fkqco7unb4h4ivx8jo6vdb5sp3r473"
                + "8zhlvrbdwfa38s3wh82lrnugrhxq8eap3rebq9dz8xka713aui4v2acquulic"
                + "dadt63cv0biz7y7eccfh1tri60526b2bmon71k29n6p29tsbhyl2wdx5hsjux"
                + "g2wd993hcndot1fc5oz8kebopqrudyf4o7tjc5ca0bvtysnw3gn64c1sd2iw2r"
                + "lhlxk7c5szp6kde8dvitteoqo1oufum5eyjbk1q2fegf9vpyng3bs6c6qfoibc2"
                + "wvxgjn4hnismbsr4ovwe5gvam74ikdromn8dxv91e5wuvcqml92jgfoj4g0xzrn"
                + "s05hsqs55a5a9ao44f6m2eccscq4crfm5dxwdl7xbmmmj1yfgpygco4mvh9ksits"
                + "xoy30v6dgse76wmyemjymyaa2f6my83vu55z9vhywv6a4har3tep32dg3mvol1arsi"
                + "a8bllis4awfqjpw57lpv1fmt5n8ns8vqvle09cpehrlkt5kjcaucwb64c25q8zvikg"
                + "tm2p0ywfnsapm97fxloymcqp0vgwmqzt3feaq8o6mzjaqmgap7r7gtn1k1awwxjs1s" + "d91g4y1emab14hs";

        licenseInfoService.updateLicense(license);
        realLicense = new File(kylinHome, "LICENSE");
        fileLicense = FileUtils.readFileToString(realLicense);

        assert fileLicense.equals(license);

    }

}
