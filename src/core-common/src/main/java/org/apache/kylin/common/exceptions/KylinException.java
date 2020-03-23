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
package org.apache.kylin.common.exceptions;

import java.io.IOException;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.response.ResponseCode;
import org.codehaus.commons.nullanalysis.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;

import io.kyligence.kap.common.util.FileUtils;
import lombok.Getter;

@Getter
public class KylinException extends RuntimeException {
    public static final Logger logger = LoggerFactory.getLogger(KylinException.class);
    private static final String enErrorCodeFile = "kylin_errorcode_conf_en.properties";
    private static final String zhErrorCodeFile = "kylin_errorcode_conf_zh.properties";
    private static ImmutableMap<String, String> enMap;
    private static ImmutableMap<String, String> zhMap;
    private static ThreadLocal<ImmutableMap<String, String>> frontMap = new ThreadLocal<>();

    private final String keCode;// for example KE-1001
    private final String code; //for example 999
    static {
        try {
            URL resource = Thread.currentThread().getContextClassLoader().getResource(enErrorCodeFile);
            Preconditions.checkNotNull(resource);
            logger.info("loading enMap {}", resource.getPath());
            enMap = ImmutableMap
                    .copyOf(new ConcurrentHashMap<>(FileUtils.readFromPropertiesFile(resource.openStream())));
            logger.info("loading enMap successful");
            resource = Thread.currentThread().getContextClassLoader().getResource(zhErrorCodeFile);
            Preconditions.checkNotNull(resource);
            logger.info("loading zhMap {}", resource.getPath());
            zhMap = ImmutableMap
                    .copyOf(new ConcurrentHashMap<>(FileUtils.readFromPropertiesFile(resource.openStream())));
            logger.info("loading zhMap successful");
            frontMap.set(enMap);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static void setMsg(String lang) {
        if ("cn".equals(lang)) {
            frontMap.set(zhMap);
        } else {
            frontMap.set(enMap);
        }
    }

    public KylinException(@NotNull String keCode, String msg) {
        super(msg);
        this.keCode = keCode;
        this.code = ResponseCode.CODE_UNDEFINED;
    }

    public KylinException(@NotNull String keCode, Throwable cause) {
        super(cause);
        this.keCode = keCode;
        this.code = ResponseCode.CODE_UNDEFINED;
    }

    public KylinException(@NotNull String keCode, String msg, Throwable cause) {
        super(msg, cause);
        this.keCode = keCode;
        this.code = ResponseCode.CODE_UNDEFINED;
    }

    public KylinException(@NotNull String keCode, String msg, String code) {
        super(msg);
        this.keCode = keCode;
        this.code = code;
    }

    public KylinException(@NotNull String keCode, String msg, String code, Throwable cause) {
        super(msg, cause);
        this.keCode = keCode;
        this.code = code;
    }

    private static ImmutableMap<String, String> getFrontMap() {
        ImmutableMap<String, String> res = frontMap.get();
        return res == null ? enMap : res;
    }

    @Override
    public String toString() {//for log
        String description = enMap.getOrDefault(keCode, "unknown");
        String detail = String.format("%s(%s)", keCode, description);
        return detail + " \n" + super.toString();
    }

    @Override
    public String getLocalizedMessage() {//for front
        String description = getFrontMap().getOrDefault(keCode, "unknown");
        return String.format("%s(%s):%s", keCode, description, super.getLocalizedMessage());
    }

}
