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

import java.io.IOException;
import java.net.DatagramSocket;
import java.net.ServerSocket;
import java.util.Random;

<<<<<<< HEAD:src/core-common/src/main/java/org/apache/kylin/common/util/CheckUtil.java
=======
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;

import org.apache.kylin.common.KylinConfig;
>>>>>>> revert KE-30046 add transactional table and ut (#25227):src/source-hive/src/main/java/org/apache/kylin/source/hive/HiveCmdBuilder.java
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CheckUtil {
    public static final Logger logger = LoggerFactory.getLogger(CheckUtil.class);

    public static boolean checkCondition(boolean condition, String message, Object... args) {
        if (condition) {
            return true;
        } else {
            logger.debug(message, args);
            return false;
        }
    }

    public static int randomAvailablePort(int minPort, int maxPort) {
        Random rand = new Random();
        for (int i = 0; i < 100; i++) {
            int p = minPort + rand.nextInt(maxPort - minPort);
            if (checkPortAvailable(p))
                return p;
        }
        throw new RuntimeException("Failed to get random available port between [" + minPort + "," + maxPort + ")");
    }

    /**
     * Checks to see if a specific port is available.
     *
     * @param port the port to check for availability
     */
    public static boolean checkPortAvailable(int port) {
        ServerSocket ss = null;
        DatagramSocket ds = null;
        try {
            tmpBeelineHqlPath = "/tmp/" + UUID.randomUUID().toString() + ".hql";
            for (String statement : statements) {
                beelineHql.append(statement.replaceAll("`", "\\\\`"));
                beelineHql.append("\n");
            }
            String createFileCmd = String.format(Locale.ROOT, CREATE_HQL_TMP_FILE_TEMPLATE, tmpBeelineHqlPath,
                    beelineHql);
            buf.append(createFileCmd);
            buf.append("\n");
            buf.append("beeline");
            buf.append(" ");
            buf.append(beelineParams);
            buf.append(" -f ");
            buf.append(tmpBeelineHqlPath);
            buf.append(";ret_code=$?;rm -f ");
            buf.append(tmpBeelineHqlPath);
            buf.append(";exit $ret_code");
        } finally {
            if (ds != null) {
                ds.close();
            }

            if (ss != null) {
                try {
                    ss.close();
                } catch (IOException e) {
                    /* should not be thrown */
                }
            }
        }

        return false;
    }
}
