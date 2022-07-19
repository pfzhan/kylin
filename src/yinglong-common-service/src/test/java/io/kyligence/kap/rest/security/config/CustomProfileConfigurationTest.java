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
package io.kyligence.kap.rest.security.config;

import io.kyligence.kap.junit.annotation.MetadataInfo;
import org.apache.kylin.rest.service.AccessService;
import org.apache.kylin.rest.service.AclService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclUtil;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.web.client.RestTemplate;

import io.kyligence.kap.rest.security.StaticAuthenticationProvider;
import io.kyligence.kap.rest.service.OpenUserGroupService;
import io.kyligence.kap.rest.service.OpenUserService;
import io.kyligence.kap.rest.service.StaticUserGroupService;
import io.kyligence.kap.rest.service.StaticUserService;

@MetadataInfo(onlyProps = true)
class CustomProfileConfigurationTest {

    private SpringApplication application;

    @BeforeEach
    void setup() {
        this.application = new SpringApplication(Config.class, CustomProfileConfiguration.class);
        this.application.setWebApplicationType(WebApplicationType.NONE);
    }

    @Test
    void testCustomProfile() {
        try (ConfigurableApplicationContext context = this.application.run("--kylin.server.mode=all",
                "--spring.main.allow-circular-references=true", "--spring.profiles.active=custom",
                "--kylin.security.custom.user-service-class-name=io.kyligence.kap.rest.service.StaticUserService",
                "--kylin.security.custom.user-group-service-class-name=io.kyligence.kap.rest.service.StaticUserGroupService",
                "--kylin.security.custom.authentication-provider-class-name=io.kyligence.kap.rest.security.StaticAuthenticationProvider")) {
            OpenUserService userService = context.getBean("userService", OpenUserService.class);
            Assertions.assertTrue(userService instanceof StaticUserService);
            OpenUserGroupService userGroupService = context.getBean("userGroupService", OpenUserGroupService.class);
            Assertions.assertTrue(userGroupService instanceof StaticUserGroupService);
            AuthenticationProvider authenticationProvider = context.getBean("customAuthProvider",
                    AuthenticationProvider.class);
            Assertions.assertTrue(authenticationProvider instanceof StaticAuthenticationProvider);
        }

    }

    @Configuration
    @Profile("custom")
    static class Config {

        @Bean("normalRestTemplate")
        public RestTemplate restTemplate() {
            return new RestTemplate();
        }

        @Bean
        public AclService aclService() {
            return new AclService();
        }

        @Bean
        public AccessService accessService() {
            return new AccessService();
        }

        @Bean
        public AclUtil aclUtil() {
            return new AclUtil();
        }

        @Bean
        public AclEvaluate aclEvaluate() {
            return new AclEvaluate();
        }
    }

}