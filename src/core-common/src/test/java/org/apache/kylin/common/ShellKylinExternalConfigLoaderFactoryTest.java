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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junitpioneer.jupiter.SetEnvironmentVariable;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import io.kyligence.config.core.loader.IExternalConfigLoader;
import io.kyligence.config.external.loader.NacosExternalConfigLoader;
import io.kyligence.kap.junit.annotation.MetadataInfo;

@MetadataInfo
class ShellKylinExternalConfigLoaderFactoryTest {

    @Test
    void testGetConfigLoader() {
        IExternalConfigLoader configLoader = ShellKylinExternalConfigLoaderFactory.getConfigLoader();
        Assertions.assertInstanceOf(KylinExternalConfigLoader.class, configLoader);
    }

    @Test
    void testGetConfigLoaderByConfigYaml() throws IOException {
        Path confPath = Paths.get(KylinConfig.getKylinHome(), "conf");
        confPath.toFile().mkdir();
        Path configFile = Files.createFile(Paths.get(KylinConfig.getKylinHome(), "conf", "config.yaml"));

        String content = "spring:\n" + "  cloud:\n" + "    nacos:\n" + "      config:\n"
                + "        server-addr: ${NACOS_CONFIG_SERVER_ADDR}\n" + "\n" + "kylin:\n" + "  external:\n"
                + "    config:\n" + "      infos:\n" + "        - target: org.apache.kylin.common.KylinConfig\n"
                + "          type: nacos\n" + "          properties:\n"
                + "            app: \"${APP_NAME:ShellKylinExternalConfigLoaderFactoryTest}\"\n"
                + "            zhName: \"${APP_DISPLAY_NAME}\"\n"
                + "            dataIds: \"${APP_NAME:yinglong-common-booter}-kylin-config\"\n"
                + "            group: \"${TENANT_ID}\"\n" + "            autoRefresh: true\n"
                + "            needInit: true\n"
                + "            initConfigContent: \"${KYLIN_HOME}/conf/init.properties\"\n"
                + "            configPropertyFileType: \"PROPERTIES\"\n"
                + "            configLibrary: \"${KYLIN_HOME}/conf/config_library.csv\"";

        Files.write(configFile, content.getBytes(StandardCharsets.UTF_8));
        try (MockedConstruction<NacosExternalConfigLoader> mockedConstruction = Mockito
                .mockConstruction(NacosExternalConfigLoader.class)) {
            IExternalConfigLoader configLoader = ShellKylinExternalConfigLoaderFactory.getConfigLoader();
            Assertions.assertInstanceOf(NacosExternalConfigLoader.class, configLoader);
        }
    }

    @Test
    @SetEnvironmentVariable(key = KylinConfig.USE_LEGACY_CONFIG, value = "true")
    void testGetConfigLoaderWithConfigYamlAndUSE_LEGACY_CONFIG() throws IOException {
        Path confPath = Paths.get(KylinConfig.getKylinHome(), "conf");
        confPath.toFile().mkdir();
        Path configFile = Files.createFile(Paths.get(KylinConfig.getKylinHome(), "conf", "config.yaml"));

        String content = "spring:\n" + "  cloud:\n" + "    nacos:\n" + "      config:\n"
                + "        server-addr: ${NACOS_CONFIG_SERVER_ADDR}\n" + "\n" + "kylin:\n" + "  external:\n"
                + "    config:\n" + "      infos:\n" + "        - target: org.apache.kylin.common.KylinConfig\n"
                + "          type: nacos\n" + "          properties:\n"
                + "            app: \"${APP_NAME:ShellKylinExternalConfigLoaderFactoryTest}\"\n"
                + "            zhName: \"${APP_DISPLAY_NAME}\"\n"
                + "            dataIds: \"${APP_NAME:yinglong-common-booter}-kylin-config\"\n"
                + "            group: \"${TENANT_ID}\"\n" + "            autoRefresh: true\n"
                + "            needInit: true\n"
                + "            initConfigContent: \"${KYLIN_HOME}/conf/init.properties\"\n"
                + "            configPropertyFileType: \"PROPERTIES\"\n"
                + "            configLibrary: \"${KYLIN_HOME}/conf/config_library.csv\"";

        Files.write(configFile, content.getBytes(StandardCharsets.UTF_8));
        try (MockedConstruction<NacosExternalConfigLoader> mockedConstruction = Mockito
                .mockConstruction(NacosExternalConfigLoader.class)) {
            IExternalConfigLoader configLoader = ShellKylinExternalConfigLoaderFactory.getConfigLoader();
            Assertions.assertInstanceOf(KylinExternalConfigLoader.class, configLoader);
        }
    }
}