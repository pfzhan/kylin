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
package org.apache.kylin.rest.util;

import static org.junit.Assert.assertNotNull;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.query.util.RawSqlParser;
import org.apache.kylin.rest.response.SQLResponse;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TableauInterceptorTest {

    Logger logger = LoggerFactory.getLogger(TableauInterceptorTest.class);

    @Test
    public void testTableauIntercept() throws IOException {
        List<String> sqls = Files.walk(Paths.get("../kap-it/src/test/resources/query/tableau_probing"))
                .filter(file -> Files.isRegularFile(file)).map(path -> {
                    try {
                        String sql = new String(Files.readAllBytes(path), StandardCharsets.UTF_8);
                        return new RawSqlParser(sql).parse().getStatementString();
                    } catch (Exception e) {
                        logger.error("meeting error when reading sqls in dir tableau_probing", e);
                        return null;
                    }
                }).filter(sql -> sql != null || sql.startsWith("SELECT")).collect(Collectors.toList());

        for (String sql : sqls) {
            SQLResponse response = TableauInterceptor.tableauIntercept(sql);
            assertNotNull(response);
        }
    }

}
