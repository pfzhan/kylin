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

import java.util.Properties;

import org.apache.kylin.common.KylinConfig;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.context.config.ConfigDataEnvironmentPostProcessor;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.PropertySource;

import io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil;
import lombok.val;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class KylinPropertySourceConfiguration implements EnvironmentPostProcessor, Ordered {

    @Override
    public void postProcessEnvironment(ConfigurableEnvironment environment, SpringApplication application) {

        if (environment.getPropertySources().contains("bootstrap")) {
            return;
        }

        log.debug("use kylinconfig as spring properties");
        val propertySources = environment.getPropertySources();
        val kylinConfig = KylinConfig.getInstanceFromEnv();
        val storageURL = kylinConfig.getMetadataUrl();
        if (storageURL.getScheme().equals("jdbc")) {
            JdbcUtil.datasourceParameters(storageURL).forEach((key, value) -> {
                kylinConfig.setProperty("spring.datasource." + key, value.toString());
            });
        }
        PropertySource<String> source = new PropertySource<String>("kylin") {
            Properties properties = KylinConfig.getInstanceFromEnv().exportToProperties();

            @Override
            public Object getProperty(String name) {
                return properties.getProperty(name);
            }
        };
        propertySources.addAfter("systemProperties", source);
    }

    @Override
    public int getOrder() {
        return ConfigDataEnvironmentPostProcessor.ORDER + 1020;
    }
}
