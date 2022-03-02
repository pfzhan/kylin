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
package io.kyligence.kap.rest;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.DefaultBootstrapContext;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.boot.env.EnvironmentPostProcessorsFactory;
import org.springframework.boot.logging.DeferredLogs;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.env.Profiles;

import io.kyligence.kap.rest.config.KylinPropertySourceConfiguration;

class KylinPrepareEnvListenerTest {

    private SpringApplication application;

    @BeforeEach
    void setup() {
        this.application = new SpringApplication(Config.class);
        this.application.setWebApplicationType(WebApplicationType.NONE);
    }

    @Test
    void testNotDefaultActiveProfile() {
        try (ConfigurableApplicationContext context = this.application.run("--kylin.server.mode=all")) {
            Assertions.assertTrue(context.getEnvironment().acceptsProfiles(Profiles.of("dev")));
        }
    }

    @Test
    void testEnvironmentProcessOrder() {
        try (ConfigurableApplicationContext context = this.application.run("--kylin.server.mode=all")) {
            List<EnvironmentPostProcessor> processors = EnvironmentPostProcessorsFactory
                    .fromSpringFactories(application.getClassLoader())
                    .getEnvironmentPostProcessors(new DeferredLogs(), new DefaultBootstrapContext());
            Assertions.assertFalse(processors.isEmpty());
            System.out.println(processors.stream().map(EnvironmentPostProcessor::getClass).map(Class::getSimpleName)
                    .collect(Collectors.joining(",")));

            Map<? extends Class<? extends EnvironmentPostProcessor>, Integer> classIndexMap = processors.stream()
                    .collect(Collectors.toMap(EnvironmentPostProcessor::getClass, processors::indexOf));

            Integer configDataEnvIndex = classIndexMap.get(ConfigDataEnvironmentPostProcessor.class);
            Integer kylinPrepareEnvIndex = classIndexMap.get(KylinPrepareEnvListener.class);
            Integer kylinPropertySourceIndex = classIndexMap.get(KylinPropertySourceConfiguration.class);

            Assertions.assertTrue(configDataEnvIndex < kylinPrepareEnvIndex);
            Assertions.assertTrue(kylinPrepareEnvIndex < kylinPropertySourceIndex);
        }
    }

    @Configuration
    static class Config {

    }

}