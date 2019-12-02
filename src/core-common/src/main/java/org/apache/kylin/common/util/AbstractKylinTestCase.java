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

package org.apache.kylin.common.util;

import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;

import com.google.common.collect.Maps;

import lombok.extern.slf4j.Slf4j;

/**
 * @author ysong1
 *
 */
@Slf4j
public abstract class AbstractKylinTestCase {

    static {
        System.setProperty("needCheckCC", "true");
    }

    private Map<String, String> systemProp = Maps.newHashMap();

    protected void overwriteSystemProp(String key, String value) {
        systemProp.put(key, System.getProperty(key));
        System.setProperty(key, value);
    }

    protected void restoreAllSystemProp() {
        systemProp.forEach((prop, value) -> {
            if (value == null) {
                log.info("Clear {}", prop);
                System.clearProperty(prop);
            } else {
                log.info("restore {}", prop);
                System.setProperty(prop, value);
            }
        });
        systemProp.clear();
    }

    public abstract void createTestMetadata(String... overlayMetadataDirs) throws Exception;

    public abstract void cleanupTestMetadata() throws Exception;

    public static KylinConfig getTestConfig() {
        return KylinConfig.getInstanceFromEnv();
    }

    public static void clearTestConfig() {
        try {
            ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv()).close();
        } catch (Exception ignore) {
        }
        System.clearProperty(KylinConfig.KYLIN_CONF);
        KylinConfig.destroyInstance();
    }

}
