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
import static org.apache.kylin.common.KylinConfigBase.BCC;
import static org.apache.kylin.common.exception.CommonErrorCode.UNKNOWN_ERROR_CODE;
import static org.apache.kylin.common.exception.ServerErrorCode.FILE_NOT_EXIST;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Map;
import java.util.Properties;

import javax.annotation.Nonnull;

import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.util.OrderedProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.kyligence.config.core.loader.IExternalConfigLoader;

public class KylinExternalConfigLoader implements IExternalConfigLoader {

    private static final long serialVersionUID = 1694879531312203159L;

    private static final Logger logger = LoggerFactory.getLogger(KylinExternalConfigLoader.class);

    public static final String KYLIN_CONF_PROPERTIES_FILE = "kylin.properties";

    private final File propFile;

    private final Properties properties;

    public KylinExternalConfigLoader(Map<String, String> map) {
        this(map.get("config-dir") == null ? getSitePropertiesFile() : new File(map.get("config-dir")));
    }

    public KylinExternalConfigLoader(File file) {
        this.propFile = file;
        this.properties = loadProperties();
    }

    private Properties loadProperties() {
        Properties siteProperties = new Properties();
        OrderedProperties orderedProperties = buildSiteOrderedProps();
        for (Map.Entry<String, String> each : orderedProperties.entrySet()) {
            siteProperties.put(each.getKey(), each.getValue());
        }

        return siteProperties;
    }

    // build kylin properties from site deployment, a.k.a. KYLIN_HOME/conf/kylin.properties
    private OrderedProperties buildSiteOrderedProps() {
        try {
            // 1. load default configurations from classpath.
            // we have a kylin-defaults.properties in kylin/core-common/src/main/resources
            URL resource = Thread.currentThread().getContextClassLoader().getResource("kylin-defaults.properties");
            Preconditions.checkNotNull(resource);
            logger.info("Loading kylin-defaults.properties from {}", resource.getPath());
            OrderedProperties orderedProperties = new OrderedProperties();
            loadAndTrimProperties(resource.openStream(), orderedProperties);

            for (int i = 0; i < 10; i++) {
                String fileName = "kylin-defaults" + (i) + ".properties";
                URL additionalResource = Thread.currentThread().getContextClassLoader().getResource(fileName);
                if (additionalResource != null) {
                    logger.info("Loading {} from {} ", fileName, additionalResource.getPath());
                    loadAndTrimProperties(additionalResource.openStream(), orderedProperties);
                }
            }

            // 2. load site conf, to keep backward compatibility it's still named kylin.properties
            // actually it's better to be named kylin-site.properties
            if (propFile == null || !propFile.exists()) {
                logger.error("fail to locate " + KYLIN_CONF_PROPERTIES_FILE);
                throw new KylinConfigCannotInitException("fail to locate " + KYLIN_CONF_PROPERTIES_FILE);
            }
            loadAndTrimProperties(Files.newInputStream(propFile.toPath()), orderedProperties);

            // 3. still support kylin.properties.override as secondary override
            // not suggest to use it anymore
            File propOverrideFile = new File(propFile.getParentFile(), propFile.getName() + ".override");
            if (propOverrideFile.exists()) {
                loadAndTrimProperties(Files.newInputStream(propOverrideFile.toPath()), orderedProperties);
            }

            return orderedProperties;
        } catch (IOException e) {
            throw new KylinException(FILE_NOT_EXIST, e);
        }
    }

    private static OrderedProperties loadPropertiesFromInputStream(InputStream inputStream) {
        try (BufferedReader confReader = new BufferedReader(
                new InputStreamReader(inputStream, StandardCharsets.UTF_8))) {
            OrderedProperties temp = new OrderedProperties();
            temp.load(confReader);
            return temp;
        } catch (Exception e) {
            throw new KylinException(FILE_NOT_EXIST, e);
        }
    }

    /**
     * 1.load props from InputStream
     * 2.trim all key-value
     * 3.backward compatibility check
     * 4.close the passed in inputStream
     * @param inputStream
     * @param properties
     */
    private static void loadAndTrimProperties(@Nonnull InputStream inputStream, @Nonnull OrderedProperties properties) {
        Preconditions.checkNotNull(inputStream);
        Preconditions.checkNotNull(properties);
        try {
            OrderedProperties trimProps = OrderedProperties.copyAndTrim(loadPropertiesFromInputStream(inputStream));
            properties.putAll(BCC.check(trimProps));
        } catch (Exception e) {
            throw new KylinException(UNKNOWN_ERROR_CODE, " loadAndTrimProperties error ", e);
        }
    }

    @Override
    public String getConfig() {
        StringWriter writer = new StringWriter();
        try {
            properties.store(new PrintWriter(writer), "");
        } catch (IOException e) {
            e.printStackTrace();
        }
        return writer.toString();
    }

    @Override
    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    @Override
    public Properties getProperties() {
        return this.properties;
    }
}
