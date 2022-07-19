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

package io.kyligence.kap.rest.config;

import io.kyligence.kap.junit.annotation.MetadataInfo;
import io.kyligence.kap.junit.annotation.OverwriteProp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;

@OverwriteProp(key = "kylin.metadata.url", value = "ke_metadata@jdbc,driverClassName=org.postgresql.Driver,url=jdbc:postgresql://sandbox:5432/kylin")
@MetadataInfo(onlyProps = true)
class KylinPropertySourceConfigurationTest {
    private SpringApplication application;

    @BeforeEach
    void setup() {
        this.application = new SpringApplication(Config.class);
        this.application.setWebApplicationType(WebApplicationType.NONE);
    }

    @Test
    void testKylinPropertySource() {
        try (ConfigurableApplicationContext context = this.application.run("--kylin.server.mode=all")) {
            ConfigurableEnvironment environment = context.getEnvironment();
            PropertySource<?> kylinPropertySource = environment.getPropertySources().get("kylin");
            Assertions.assertNotNull(kylinPropertySource);
            String profile = environment.getProperty("kylin.security.profile");
            Assertions.assertNotNull(profile);
        }
    }

    @Configuration
    static class Config {

    }
}