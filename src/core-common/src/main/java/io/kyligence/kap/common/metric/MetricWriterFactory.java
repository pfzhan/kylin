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

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetricWriterFactory {
    public static final Logger logger = LoggerFactory
            .getLogger(io.kyligence.kap.common.metric.MetricWriterFactory.class);


    public static MetricWriter getInstance(String type) throws Exception {
        if (StringUtils.isBlank(type)) {
            type = MetricWriter.Type.BLACK_HOLE.name();
        }
        if (type.equalsIgnoreCase(MetricWriter.Type.INFLUX.name())) {
            return InfluxDBWriter.getInstance();
        } else if (type.equalsIgnoreCase(MetricWriter.Type.RDBMS.name())) {
            return RDBMSWriter.getInstance();
        } else if (type.equalsIgnoreCase(MetricWriter.Type.CONSOLE.name())) {
            return ConsoleWriter.INSTANCE;
        } else {
            return BlackHoleWriter.INSTANCE;
        }
    }
}
