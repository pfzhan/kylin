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

package io.kyligence.kap.tool;

import java.util.Map;
import java.util.Objects;
import java.util.Properties;

import io.kyligence.kap.common.util.EncryptUtil;
import org.apache.kylin.common.BackwardCompatibilityConfig;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.obf.IKeep;

public class KylinConfigCLI implements IKeep {
    public static void main(String[] args) {
        boolean needDec = false;
        if (args.length != 1) {
            if (args.length < 2 || !Objects.equals(EncryptUtil.DEC_FLAG, args[1])) {
                System.err.println("Usage: KylinConfigCLI conf_name");
                System.err.println("Example: KylinConfigCLI kylin.server.mode");
                System.exit(1);
            } else {
                needDec = true;
            }
        }

        Properties config = KylinConfig.getInstanceFromEnv().exportToProperties();

        BackwardCompatibilityConfig bcc = new BackwardCompatibilityConfig();
        String key = bcc.check(args[0].trim());
        if (!key.endsWith(".")) {
            String value = config.getProperty(key);
            if (value == null) {
                value = "";
            }
            if (needDec && EncryptUtil.isEncrypted(value)) {
                System.out.println(EncryptUtil.decryptPassInKylin(value));
            } else {
                System.out.println(value.trim());
            }
        } else {
            Map<String, String> props = getPropertiesByPrefix(config, key);
            for (Map.Entry<String, String> prop : props.entrySet()) {
                System.out.println(prop.getKey() + "=" + prop.getValue().trim());
            }
        }
    }

    static private Map<String, String> getPropertiesByPrefix(Properties props, String prefix) {
        Map<String, String> result = Maps.newLinkedHashMap();
        for (Map.Entry<Object, Object> entry : props.entrySet()) {
            String entryKey = (String) entry.getKey();
            if (entryKey.startsWith(prefix)) {
                result.put(entryKey.substring(prefix.length()), (String) entry.getValue());
            }
        }
        return result;
    }
}
