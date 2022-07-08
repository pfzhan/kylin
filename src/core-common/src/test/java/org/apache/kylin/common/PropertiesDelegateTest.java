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

import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class PropertiesDelegateTest {

    private PropertiesDelegate delegate;

    @BeforeEach
    void setup() {
        Properties properties = new Properties();
        properties.put("key_in_prop", "v0");
        properties.put("key_override_external", "v1");

        Properties externalProperties = new Properties();
        externalProperties.put("key_override_external", "v11");
        externalProperties.put("key_in_external", "v2");

        TestExternalConfigLoader testExternalConfigLoader = new TestExternalConfigLoader(externalProperties);
        delegate = new PropertiesDelegate(properties, testExternalConfigLoader);
    }

    @Test
    void testReloadProperties() {
        Properties properties = new Properties();
        delegate.reloadProperties(properties);
        Assertions.assertNull(delegate.getProperty("key_in_prop"));
        Assertions.assertEquals("v11", delegate.getProperty("key_override_external"));

        properties.put("key_override_external", "v1_1");
        delegate.reloadProperties(properties);
        Assertions.assertEquals("v1_1", delegate.getProperty("key_override_external"));
    }

    @Test
    void testGetProperty() {
        Assertions.assertEquals("v0", delegate.getProperty("key_in_prop"));
        Assertions.assertEquals("v1", delegate.getProperty("key_override_external"));
        Assertions.assertEquals("v2", delegate.getProperty("key_in_external"));
        Assertions.assertEquals("v2", delegate.getProperty("key_in_external", "default_value"));

        Assertions.assertNull(delegate.getProperty("key_none_exists"));
        Assertions.assertEquals("default_value", delegate.getProperty("key_none_exists", "default_value"));
    }

    @Test
    void testPut() {
        delegate.put("key_in_prop", "update_v0");
        Assertions.assertEquals("update_v0", delegate.getProperty("key_in_prop"));

        delegate.put("key_override_external", "update_v1");
        Assertions.assertEquals("update_v1", delegate.getProperty("key_override_external"));

        delegate.put("key_in_external", "update_v2");
        Assertions.assertEquals("update_v2", delegate.getProperty("key_in_external"));
    }

    @Test
    void testSetProperty() {
        delegate.setProperty("key_in_prop", "update_v0");
        Assertions.assertEquals("update_v0", delegate.getProperty("key_in_prop"));

        delegate.setProperty("key_override_external", "update_v1");
        Assertions.assertEquals("update_v1", delegate.getProperty("key_override_external"));

        delegate.setProperty("key_in_external", "update_v2");
        Assertions.assertEquals("update_v2", delegate.getProperty("key_in_external"));
    }

    @Test
    void testSize() {
        Assertions.assertEquals(3, delegate.size());
    }

    @Test
    void testEntrySet() {
        Set<Map.Entry<Object, Object>> entries = delegate.entrySet();
        Assertions.assertEquals(3, entries.size());
    }

    @Test
    void testKeys() {
        List<String> keys = new ArrayList<>();
        Enumeration<Object> enumer = delegate.keys();
        while (enumer.hasMoreElements()) {
            keys.add((String) enumer.nextElement());
        }

        Assertions.assertEquals(3, keys.size());

        Assertions.assertEquals("key_in_external, key_in_prop, key_override_external",
                keys.stream().sorted().collect(Collectors.joining(", ")));
    }

    @Test
    void testEqualsAndHashCode() {
        Set<Properties> sets = new HashSet<>();
        sets.add(delegate);
        Assertions.assertEquals(1, sets.size());

        sets.add(delegate);
        Assertions.assertEquals(1, sets.size());

        Assertions.assertEquals(delegate, delegate);

        delegate.reloadProperties(new Properties());
        sets.add(delegate);
        Assertions.assertEquals(2, sets.size());

        Properties properties = new Properties();
        sets.add(properties);
        Assertions.assertEquals(3, sets.size());

        sets.add(properties);
        Assertions.assertEquals(3, sets.size());

        properties.put("1", "v1");
        sets.add(properties);
        Assertions.assertEquals(4, sets.size());
    }
}