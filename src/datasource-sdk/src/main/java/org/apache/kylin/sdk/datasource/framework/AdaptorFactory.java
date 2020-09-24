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
package org.apache.kylin.sdk.datasource.framework;

import java.lang.reflect.Constructor;

import org.apache.kylin.sdk.datasource.adaptor.AbstractJdbcAdaptor;
import org.apache.kylin.sdk.datasource.adaptor.AdaptorConfig;

public class AdaptorFactory {
    public static AbstractJdbcAdaptor createJdbcAdaptor(String adaptorClazz, AdaptorConfig jdbcConf) throws Exception {
        Constructor<?>[] list = Class.forName(adaptorClazz).getConstructors();
        for (Constructor<?> c : list) {
            if (c.getParameterTypes().length == 1) {
                if (c.getParameterTypes()[0] == AdaptorConfig.class) {
                    return (AbstractJdbcAdaptor) c.newInstance(jdbcConf); // adaptor with kylin AdaptorConfig
                } else {
                    // Compatible with old adaptors with kap AdaptorConfig
                    String configClassName = "io.kyligence.kap.sdk.datasource.adaptor.AdaptorConfig";
                    AdaptorConfig conf = (AdaptorConfig) Class.forName(configClassName)
                            .getConstructor(String.class, String.class, String.class, String.class)
                            .newInstance(jdbcConf.url, jdbcConf.driver, jdbcConf.username, jdbcConf.password);
                    conf.poolMaxIdle = jdbcConf.poolMaxIdle;
                    conf.poolMinIdle = jdbcConf.poolMinIdle;
                    conf.poolMaxTotal = jdbcConf.poolMaxTotal;
                    conf.datasourceId = jdbcConf.datasourceId;
                    return (AbstractJdbcAdaptor) c.newInstance(conf);
                }
            }
        }
        throw new NoSuchMethodException();
    }
}
