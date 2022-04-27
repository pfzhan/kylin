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
package org.apache.kylin.common.exception.code;

import java.io.IOException;
import java.io.Serializable;
import java.net.URL;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.kylin.common.exception.ErrorCodeException;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import io.kyligence.kap.common.util.FileUtils;
import io.kyligence.kap.common.util.ResourceUtils;
import lombok.extern.slf4j.Slf4j;

/**
 * The new ErrorCode will gradually replace org.apache.kylin.common.exception.ErrorCode
 */
@Slf4j
public class ErrorCode implements Serializable {

    private static final String ERROR_CODE_FILE = "kylin_errorcode_conf.properties";
    private static final ImmutableSet<String> CODE_SET;

    static {
        try {
            URL resource = ResourceUtils.getServerConfUrl(ERROR_CODE_FILE);
            log.info("loading error code {}", resource.getPath());
            CODE_SET = ImmutableMap
                    .copyOf(new ConcurrentHashMap<>(FileUtils.readFromPropertiesFile(resource.openStream()))).keySet();
            log.info("loading error code successful");
        } catch (IOException e) {
            throw new ErrorCodeException("loading error code failed.", e);
        }
    }

    private final String keCode;

    public ErrorCode(String keCode) {
        if (!CODE_SET.contains(keCode)) {
            throw new ErrorCodeException("Error code [" + keCode + "] must be defined in the error code file");
        }
        this.keCode = keCode;
    }

    public String getCode() {
        return this.keCode;
    }

}
