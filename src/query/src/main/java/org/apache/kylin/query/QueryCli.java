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

package org.apache.kylin.query;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.DBUtils;

public class QueryCli {

    @SuppressWarnings("static-access")
    private static final Option OPTION_METADATA = OptionBuilder.withArgName("metadata url").hasArg().isRequired()
            .withDescription("Metadata URL").create("metadata");

    @SuppressWarnings("static-access")
    private static final Option OPTION_SQL = OptionBuilder.withArgName("input sql").hasArg().isRequired()
            .withDescription("SQL").create("sql");

    public static void main(String[] args) throws Exception {

        Options options = new Options();
        options.addOption(OPTION_METADATA);
        options.addOption(OPTION_SQL);

        CommandLineParser parser = new GnuParser();
        CommandLine commandLine = parser.parse(options, args);
        KylinConfig config = KylinConfig.createInstanceFromUri(commandLine.getOptionValue(OPTION_METADATA.getOpt()));
        String sql = commandLine.getOptionValue(OPTION_SQL.getOpt());

        Connection conn = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {

            // remove since this class looks deprecated
//            stmt = conn.createStatement();
//            rs = stmt.executeQuery(sql);
//            int n = 0;
//            ResultSetMetaData meta = rs.getMetaData();
//            while (rs.next()) {
//                n++;
//                for (int i = 1; i <= meta.getColumnCount(); i++) {
//                    System.out.println(n + " - " + meta.getColumnLabel(i) + ":\t" + rs.getObject(i));
//                }
//            }
        } finally {
            DBUtils.closeQuietly(rs);
            DBUtils.closeQuietly(stmt);
            DBUtils.closeQuietly(conn);
        }

    }
}