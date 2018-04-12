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
package org.apache.kylin.common.htrace;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.shaded.htrace.org.apache.htrace.HTraceConfiguration;
import org.apache.kylin.shaded.htrace.org.apache.htrace.SpanReceiver;
import org.apache.kylin.shaded.htrace.org.apache.htrace.Trace;
import org.apache.kylin.shaded.htrace.org.apache.htrace.impl.LocalFileSpanReceiver;
import org.apache.kylin.shaded.htrace.org.apache.htrace.impl.StandardOutSpanReceiver;
import org.apache.kylin.shaded.htrace.org.apache.htrace.impl.ZipkinSpanReceiver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

public class HtraceInit {
    public static final Logger logger = LoggerFactory.getLogger(HtraceInit.class);

    static {
        try {
            // init for HTrace
            String fileName = System.getProperty("spanFile");

            Collection<SpanReceiver> rcvrs = new HashSet<SpanReceiver>();

            // writes spans to a file if one is provided to maven with
            // -DspanFile="FILENAME", otherwise writes to standard out.
            if (fileName != null) {
                File f = new File(fileName);
                File parent = f.getParentFile();
                if (parent != null && !parent.exists() && !parent.mkdirs()) {
                    throw new IllegalArgumentException("Couldn't create file: " + fileName);
                }
                HashMap<String, String> conf = new HashMap<String, String>();
                conf.put("local-file-span-receiver.path", fileName);
                LocalFileSpanReceiver receiver = new LocalFileSpanReceiver(HTraceConfiguration.fromMap(conf));
                rcvrs.add(receiver);
            } else {
                rcvrs.add(new StandardOutSpanReceiver(HTraceConfiguration.EMPTY));
            }

            String hostKey = "zipkin.collector-hostname";
            String host = System.getProperty(hostKey);
            String portKey = "zipkin.collector-port";
            String port = System.getProperty(portKey);

            Map<String, String> confMap = Maps.newHashMap();
            if (!StringUtils.isEmpty(host)) {
                confMap.put(hostKey, host);
                logger.info("{} is set to {}", hostKey, host);
            }
            if (!StringUtils.isEmpty(port)) {
                confMap.put(portKey, port);
                logger.info("{} is set to {}", portKey, port);
            }

            ZipkinSpanReceiver zipkinSpanReceiver = new ZipkinSpanReceiver(HTraceConfiguration.fromMap(confMap));
            rcvrs.add(zipkinSpanReceiver);

            for (SpanReceiver receiver : rcvrs) {
                Trace.addReceiver(receiver);
                logger.info("SpanReceiver {} added.", receiver);
            }
        } catch (Exception e) {
            //
            logger.error("Failed to init HTrace", e);
        }
    }

    public static void init() { //only to trigger static block
    }
}
