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

package io.kyligence.kap.rest.controller;

import java.io.IOException;

import org.apache.kylin.rest.service.LicenseInfoService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.InjectMocks;
import org.mockito.Mockito;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.rest.rules.ClearKEPropertiesRule;

public class NSystemControllerTest extends NLocalFileMetadataTestCase {

    @Rule
    public ClearKEPropertiesRule clearKEProperties = new ClearKEPropertiesRule();

    @Before
    public void setupResource() {
        createTestMetadata();
    }

    @InjectMocks
    private LicenseInfoService licenseInfoService = Mockito.spy(new LicenseInfoService());

    @After
    public void tearDown() {
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

}
