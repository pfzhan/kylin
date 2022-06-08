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

import java.util.Arrays;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.constant.Constant;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.security.authentication.TestingAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.web.WebAppConfiguration;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import io.kyligence.kap.engine.spark.ExecutableUtils;
import io.kyligence.kap.metadata.user.ManagedUser;
import io.kyligence.kap.secondstorage.SecondStorageUpdater;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = ServiceTestBase.SpringConfig.class)
@WebAppConfiguration(value = "../yinglong-common-service/src/main/resources")
@TestPropertySource(properties = {"spring.cloud.nacos.discovery.enabled = false"})
@ActiveProfiles({ "testing", "test" })
@PowerMockIgnore({ "org.w3c.*", "javax.xml.*", "org.xml.*", "org.apache.*", "org.w3c.dom.*", "org.apache.cxf.*" })
public class ServiceTestBase extends NLocalFileMetadataTestCase {

    @Autowired
    @Qualifier("userService")
    UserService userService;

    @BeforeClass
    public static void setupResource() {
        staticCreateTestMetadata();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
    }

    @AfterClass
    public static void tearDownResource() {
        staticCleanupTestMetadata();
    }

    @Before
    public void setup() {
        // init job factory
        ExecutableUtils.initJobFactory();
        createTestMetadata();
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        Authentication authentication = new TestingAuthenticationToken("ADMIN", "ADMIN", Constant.ROLE_ADMIN);
        SecurityContextHolder.getContext().setAuthentication(authentication);
        if (!userService.userExists("ADMIN")) {
            userService.createUser(new ManagedUser("ADMIN", "KYLIN", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ADMIN), new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                    new SimpleGrantedAuthority(Constant.ROLE_MODELER))));
        }

        if (!userService.userExists("MODELER")) {
            userService.createUser(new ManagedUser("MODELER", "MODELER", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ANALYST),
                    new SimpleGrantedAuthority(Constant.ROLE_MODELER))));
        }

        if (!userService.userExists("ANALYST")) {
            userService.createUser(new ManagedUser("ANALYST", "ANALYST", false, Arrays.asList(//
                    new SimpleGrantedAuthority(Constant.ROLE_ANALYST))));
        }
    }

    @After
    public void cleanup() {
        cleanupTestMetadata();
    }

    /**
     * better keep this method, otherwise cause error
     * org.apache.kylin.rest.service.TestBase.initializationError
     */
    @Test
    public void test() {
    }

    @Configuration
    @ComponentScan("io.kyligence.kap.rest")
    @ImportResource(locations = { "classpath:applicationContext.xml", "classpath:kylinSecurity.xml" })
    @EnableAsync
    public static class SpringConfig {
        @Bean
        public SecondStorageUpdater getSecondStorageUpdater() {
            return Mockito.mock(SecondStorageUpdater.class, invocationOnMock -> "");
        }
    }
}
