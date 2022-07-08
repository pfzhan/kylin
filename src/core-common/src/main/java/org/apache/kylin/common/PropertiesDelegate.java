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

import java.util.Enumeration;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import io.kyligence.config.core.loader.IExternalConfigLoader;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode
public class PropertiesDelegate extends Properties {

    @EqualsAndHashCode.Include
    private final Properties properties;

    @EqualsAndHashCode.Include
    private final transient IExternalConfigLoader configLoader;

    public PropertiesDelegate(Properties properties, IExternalConfigLoader configLoader) {
        this.properties = properties;
        this.configLoader = configLoader;
    }

    public synchronized void reloadProperties(Properties properties) {
        this.properties.clear();
        this.properties.putAll(properties);
    }

    @Override
    public String getProperty(String key) {
        String property = this.properties.getProperty(key);
        if (property == null && this.configLoader != null) {
            return configLoader.getProperty(key);
        }
        return property;
    }

    @Override
    public String getProperty(String key, String defaultValue) {
        String property = this.getProperty(key);
        if (property == null) {
            return defaultValue;
        }
        return property;
    }

    @Override
    public synchronized Object put(Object key, Object value) {
        return this.properties.put(key, value);
    }

    @Override
    public synchronized Object setProperty(String key, String value) {
        return this.put(key, value);
    }

    @Override
    public Set<Map.Entry<Object, Object>> entrySet() {
        return getAllProperties().entrySet();
    }

    @Override
    public synchronized int size() {
        return getAllProperties().size();
    }

    @Override
    public synchronized Enumeration<Object> keys() {
        return getAllProperties().keys();
    }

    private synchronized Properties getAllProperties() {
        Properties propertiesView = new Properties();
        if (this.configLoader != null) {
            propertiesView.putAll(this.configLoader.getProperties());
        }
        propertiesView.putAll(this.properties);
        return propertiesView;
    }
}
