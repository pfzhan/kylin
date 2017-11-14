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

package io.kyligence.kap.common.persistence;

import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.IOUtils;

public class JDBCSqlQueryFormatProvider {
    static Map<String, Properties> cache = new HashMap<>();

    public static JDBCSqlQueryFormat createJDBCSqlQueriesFormat(String dialect) {
        String key = String.format("/metadata-jdbc-%s.properties", dialect.toLowerCase());
        if (cache.containsKey(key)) {
            return new JDBCSqlQueryFormat(cache.get(key));
        } else {
            Properties props = new Properties();
            InputStream input = null;
            try {
                input = props.getClass().getResourceAsStream(key);
                props.load(input);
                if (!props.isEmpty()) {
                    cache.put(key, props);
                }
                return new JDBCSqlQueryFormat(props);
            } catch (Exception e) {
                throw new RuntimeException(String.format("Can't find properties named %s for metastore", key), e);
            } finally {
                IOUtils.closeQuietly(input);
            }
        }

    }
}
