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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DBUtils {

    private static final Logger logger = LoggerFactory.getLogger(DBUtils.class);
    
    /**
     * Closes an <code>ResultSet</code> unconditionally.
     * <p>
     * Equivalent to {@link ResultSet#close()}, except any exceptions will be ignored.
     * This is typically used in finally blocks.
     * <p>
     *
     * @param output the ResultSet to close, may be null or already closed
     */
    public static void closeQuietly(final ResultSet rs) {
        closeQuietly((AutoCloseable) rs);
    }

    /**
     * Closes an <code>Statement</code> unconditionally.
     * <p>
     * Equivalent to {@link Statement#close()}, except any exceptions will be ignored.
     * This is typically used in finally blocks.
     * <p>
     *
     * @param output the ResultSet to close, may be null or already closed
     */
    public static void closeQuietly(final Statement stmt) {
        closeQuietly((AutoCloseable) stmt);
    }

    /**
     * Closes an <code>Connection</code> unconditionally.
     * <p>
     * Equivalent to {@link Connection#close()}, except any exceptions will be ignored.
     * This is typically used in finally blocks.
     * <p>
     *
     * @param output the ResultSet to close, may be null or already closed
     */
    public static void closeQuietly(final Connection conn) {
        closeQuietly((AutoCloseable) conn);
    }

    /**
     * Closes a <code>AutoCloseable</code> unconditionally.
     * <p>
     * Equivalent to {@link AutoCloseable#close()}, except any exceptions will be ignored. This is typically used in
     * finally blocks.
     * <p>
     *
     * @param closeable the objects to close, may be null or already closed
     */
    public static void closeQuietly(final AutoCloseable closeable) {
        try {
            if (closeable != null) {
                closeable.close();
            }
        } catch (final Exception ioe) {
            logger.debug("", ioe);
        }
    }
}
