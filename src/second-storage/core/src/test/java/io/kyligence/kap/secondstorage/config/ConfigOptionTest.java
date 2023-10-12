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
package io.kyligence.kap.secondstorage.config;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class ConfigOptionTest {

    @Test
    void test01() {
        ConfigOption<String> configOption1 = new ConfigOption<>("k1", "", String.class);
        ConfigOption<String> configOption2 = new ConfigOption<>("k1", "", String.class);

        Assertions.assertEquals(configOption1, configOption2);
        Assertions.assertNotEquals("", configOption1.toString());

        Assertions.assertTrue(configOption1.hasDefaultValue());

        Map<ConfigOption<String>, String> map = new HashMap<>();
        map.put(configOption1, configOption1.toString());
        Assertions.assertFalse(map.isEmpty());
    }
}
