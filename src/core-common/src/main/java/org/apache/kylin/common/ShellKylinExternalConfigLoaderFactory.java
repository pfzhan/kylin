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

package org.apache.kylin.common;

import static org.apache.kylin.common.KylinConfig.getSitePropertiesFile;
import static org.apache.kylin.common.KylinConfig.useLegacyConfig;
import static org.apache.kylin.common.KylinConfigBase.getKylinHome;

import java.nio.file.Paths;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.Environment;
import org.springframework.core.env.PropertySource;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.FileUrlResource;

import com.alibaba.cloud.nacos.NacosConfigProperties;

import io.kyligence.config.core.conf.ExternalConfigProperties;
import io.kyligence.config.core.loader.IExternalConfigLoader;
import io.kyligence.config.external.loader.NacosExternalConfigLoader;
import lombok.val;
import lombok.experimental.UtilityClass;

/**
 * without spring context, create config loader manually.
 * used by xxTool, xxCLI
 */
@UtilityClass
public class ShellKylinExternalConfigLoaderFactory {

    private static final long serialVersionUID = 8140984602630371871L;

    private static final Logger logger = LoggerFactory.getLogger(ShellKylinExternalConfigLoaderFactory.class);

    public static IExternalConfigLoader getConfigLoader() {
        if (useLegacyConfig()) {
            return new KylinExternalConfigLoader(getSitePropertiesFile());
        }
        IExternalConfigLoader configLoader = null;
        try {
            if (Paths.get(getKylinHome(), "conf", "config.yaml").toFile().exists()) {
                val environment = getEnvironment();

                val externalConfigProperties = Binder.get(environment)
                        .bind("kylin.external.config", ExternalConfigProperties.class).get();

                val nacosConfigProperties = Binder.get(environment)
                        .bind(NacosConfigProperties.PREFIX, NacosConfigProperties.class).get();

                val infos = externalConfigProperties.getInfos();
                configLoader = infos.stream().filter(info -> info.getTarget().equals(KylinConfig.class.getName())).map(
                        info -> new NacosExternalConfigLoader(info.getProperties(), nacosConfigProperties, environment))
                        .findAny().orElse(null);
            }
        } catch (Exception e) {
            logger.warn("Can not load external config from config.yaml, use file external config loader", e);
        }

        if (configLoader != null) {
            return configLoader;
        }

        return new KylinExternalConfigLoader(getSitePropertiesFile());
    }

    /**
     * env can get properties from system property, system env and file://${KYLIN_HOME}/config/config.yaml
     * @return
     */
    private static Environment getEnvironment() {
        StandardEnvironment environment = new StandardEnvironment();

        // file://${KYLIN_HOME}/conf/config.yaml
        try {
            YamlPropertySourceLoader loader = new YamlPropertySourceLoader();
            List<PropertySource<?>> propertySources = loader.load("config-application-yaml",
                    new FileUrlResource(Paths.get(getKylinHome(), "conf", "config.yaml").toFile().getAbsolutePath()));
            propertySources.forEach(propertySource -> environment.getPropertySources().addLast(propertySource));
        } catch (Exception e) {
            logger.warn("Can not load config.yaml", e);
        }

        return environment;
    }
}
