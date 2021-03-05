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
package io.kyligence.kap.tool.obf;

import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.StorageURL;

import com.google.common.collect.Maps;

import io.kyligence.kap.tool.constant.SensitiveConfigKeysConstant;

public class KylinConfObfuscator extends PropertiesFileObfuscator {

    private final String[] sensitiveKeywords = { "password", "user", "zookeeper.zk-auth", "source.jdbc.pass" };

    public KylinConfObfuscator(ObfLevel level, MappingRecorder recorder, ResultRecorder resultRecorder) {
        super(level, recorder, resultRecorder);
    }

    enum ACTION {
        NONE, META_JDBC, SENSITIVE_KEY;

        String masked = SensitiveConfigKeysConstant.HIDDEN;
    }

    @Override
    void obfuscateProperties(Properties input) {
        Map<Object, Object> item = Maps.newHashMap();
        for (Map.Entry<Object, Object> entry : input.entrySet()) {
            ACTION action = checkConfEntry(entry.getKey().toString(), entry.getValue().toString());
            switch (action) {
            case NONE:
                break;
            case META_JDBC:
                item.put(entry.getKey(), action.masked);
                break;
            case SENSITIVE_KEY:
                item.put(entry.getKey(), action.masked);
                break;
            default:
                break;
            }
        }
        input.putAll(item);
    }

    private ACTION checkConfEntry(String name, String value) {
        if (name.equals("kylin.metadata.url") && value.contains("@jdbc")) { // for example: jdbc password may be set in kylin.metadata.url
            ACTION act = ACTION.META_JDBC;
            try {
                act.masked = simpleToString(StorageURL.valueOf(value));
            } catch (Exception e) {
                act.masked = SensitiveConfigKeysConstant.HIDDEN;
                logger.error("Failed to parse metadata url", e);
            }
            return act;
        } else {
            for (String conf : SensitiveConfigKeysConstant.DEFAULT_SENSITIVE_CONF_BLACKLIST) {
                if (StringUtils.equals(conf, name)) {
                    return ACTION.NONE;
                }
            }
            for (String keyWord : sensitiveKeywords) {
                if ((name.contains(keyWord))) {
                    ACTION act = ACTION.SENSITIVE_KEY;
                    act.masked = SensitiveConfigKeysConstant.HIDDEN;
                    return act;
                }
            }

        }
        return ACTION.NONE;
    }

    private String simpleToString(StorageURL storageURL) {
        StringBuilder sb = new StringBuilder();
        sb.append(storageURL.getIdentifier());
        if (storageURL.getScheme() != null)
            sb.append("@").append(storageURL.getScheme());
        for (Map.Entry<String, String> kv : storageURL.getAllParameters().entrySet()) {
            String key = kv.getKey();
            String val = kv.getValue();
            if (key.equalsIgnoreCase("username") || key.equalsIgnoreCase("password")) {
                val = SensitiveConfigKeysConstant.HIDDEN;
            }
            sb.append(",").append(key).append("=");
            if (!val.isEmpty()) {
                sb.append(val);
            }
        }
        return sb.toString();
    }
}
