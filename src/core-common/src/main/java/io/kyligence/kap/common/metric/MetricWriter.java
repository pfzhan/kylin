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

package io.kyligence.kap.common.metric;

import java.util.Map;

import org.apache.commons.lang.StringUtils;

public interface MetricWriter {
    void write(String dbName, String measurement, Map<String, String> tags, Map<String, Object> metrics, long timestamp)
            throws Throwable;

    String getType();

    enum Type {
        INFLUX, BLACK_HOLE, CONSOLE
    }

    class Factory {
        public static MetricWriter getInstance(String type) {
            if (StringUtils.isBlank(type)) {
                type = Type.BLACK_HOLE.name();
            }
            if (type.equalsIgnoreCase(Type.INFLUX.name())) {
                return InfluxDBWriter.getInstance();
            } else if (type.equalsIgnoreCase(Type.CONSOLE.name())) {
                return ConsoleWriter.INSTANCE;
            } else {
                return BlackHoleWriter.INSTANCE;
            }
        }
    }
}
