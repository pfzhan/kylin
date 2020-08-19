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

package org.apache.kylin.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadFsSwitch {
    private static final Logger logger = LoggerFactory.getLogger(ReadFsSwitch.class);

    static volatile boolean fsOrFsBackup = false;
    static volatile long fsBackupCount = 0;
    static volatile long resetMillis = 0L;

    public static String select(String fileSystem, String fileSystemBackup) {
        if (resetMillis > 0 && System.currentTimeMillis() > resetMillis) {
            fsOrFsBackup = false;
            resetMillis = 0;
            logger.info("Backup read FS is back to off, switch=" + fsOrFsBackup);
        }

        if (fsOrFsBackup) {
            if (fsBackupCount++ % 100 == 0)
                logger.info("Returning backup read FS: " + fileSystemBackup + "  (empty means the origin working-dir)");

            return fileSystemBackup;
        } else {
            return fileSystem;
        }
    }

    public static void turnOnBackupFsWhile() {
        if (!fsOrFsBackup && resetMillis == 0) {
            fsOrFsBackup = true;
            fsBackupCount = 0;
            int sec = KapConfig.getInstanceFromEnv().getParquetReadFileSystemBackupResetSec();
            resetMillis = System.currentTimeMillis() + 1000 * sec; // 1 min later
            logger.info("Backup read FS is on for " + sec + " sec, switch=" + fsOrFsBackup);
        }
    }

    public static boolean turnOnSwitcherIfBackupFsAllowed(Throwable ex, String backupFsAllowedStrings) {
        // prevents repeated entrance, this method MUST NOT return true for the same query more than once
        if (!fsOrFsBackup && resetMillis == 0) {
            while (ex != null) {
                String exceptionClassName = ex.getClass().getName().toLowerCase();
                String msg = ex.getMessage() != null ? ex.getMessage().toLowerCase() : "";
                for (String backupFsAllowedString : backupFsAllowedStrings.split(",")) {
                    if (exceptionClassName.contains(backupFsAllowedString) || msg.contains(backupFsAllowedString)) {
                        turnOnBackupFsWhile();
                        return true;
                    }
                    ex = ex.getCause();
                }
            }
        }
        return false;
    }
}
