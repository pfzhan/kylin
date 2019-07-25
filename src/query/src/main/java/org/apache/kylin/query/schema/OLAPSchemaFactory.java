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

package org.apache.kylin.query.schema;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.clearspring.analytics.util.Lists;
import io.kyligence.kap.metadata.project.NProjectManager;
import org.apache.calcite.model.JsonFunction;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.commons.io.FileUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.measure.MeasureTypeFactory;
import org.apache.kylin.metadata.model.DatabaseDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

/**
 */
public class OLAPSchemaFactory implements SchemaFactory {
    public static final Logger logger = LoggerFactory.getLogger(OLAPSchemaFactory.class);

    private final static String SCHEMA_PROJECT = "project";

    @Override
    public Schema create(SchemaPlus parentSchema, String schemaName, Map<String, Object> operand) {
        String project = (String) operand.get(SCHEMA_PROJECT);
        Schema newSchema = new OLAPSchema(project, schemaName, exposeMore());
        return newSchema;
    }

    private static Map<String, File> cachedJsons = Maps.newConcurrentMap();

    public static boolean exposeMore() {
        return KylinConfig.getInstanceFromEnv().isPushDownEnabled();
    }

    public static File createTempOLAPJson(String project, KylinConfig config) throws SQLException {

        Collection<TableDesc> tables = NProjectManager.getInstance(config).listExposedTables(project, exposeMore());

        // "database" in TableDesc correspond to our schema
        // the logic to decide which schema to be "default" in calcite:
        // if some schema are named "default", use it.
        // other wise use the schema with most tables
        HashMap<String, Integer> schemaCounts = DatabaseDesc.extractDatabaseOccurenceCounts(tables);
        String majoritySchemaName = "";
        int majoritySchemaCount = 0;
        for (Map.Entry<String, Integer> e : schemaCounts.entrySet()) {
            if (e.getKey().equalsIgnoreCase("default")) {
                majoritySchemaCount = Integer.MAX_VALUE;
                majoritySchemaName = e.getKey();
            }

            if (e.getValue() >= majoritySchemaCount) {
                majoritySchemaCount = e.getValue();
                majoritySchemaName = e.getKey();
            }
        }

        try {

            StringBuilder out = new StringBuilder();
            out.append("{\n");
            out.append("    \"version\": \"1.0\",\n");
            out.append("    \"defaultSchema\": \"" + majoritySchemaName + "\",\n");
            out.append("    \"schemas\": [\n");

            int counter = 0;

            for (String schemaName : schemaCounts.keySet()) {
                out.append("        {\n");
                out.append("            \"type\": \"custom\",\n");
                out.append("            \"name\": \"" + schemaName + "\",\n");
                out.append("            \"factory\": \"" + KylinConfig.getInstanceFromEnv().getSchemaFactory() + "\",\n");
                out.append("            \"operand\": {\n");
                out.append("                \"" + SCHEMA_PROJECT + "\": \"" + project + "\"\n");
                out.append("            },\n");
                createOLAPSchemaFunctions(out);
                out.append("        }\n");

                if (++counter != schemaCounts.size()) {
                    out.append(",\n");
                }
            }

            out.append("    ]\n");
            out.append("}\n");

            String jsonContent = out.toString();
            File file = cachedJsons.get(jsonContent);
            if (file == null) {
                file = File.createTempFile("olap_model_", ".json");
                file.deleteOnExit();
                FileUtils.writeStringToFile(file, jsonContent);

                logger.debug("Adding new schema file {} to cache", file.getName());
                logger.debug("Schema json: " + jsonContent);
                cachedJsons.put(jsonContent, file);
            }

            return file;

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void createOLAPSchemaFunctions(StringBuilder out) throws Exception {
        out.append("            \"functions\": \n");
        List<JsonFunction> jsonFunctionList = Lists.newArrayList();
        //udf
        Map<String, String> udfs = Maps.newHashMap();
        udfs.putAll(KylinConfig.getInstanceFromEnv().getUDFs());
        List<JsonFunction> udfJsonFunctionList = addToJsonFunctionList(udfs, true);
        jsonFunctionList.addAll(udfJsonFunctionList);
        //udaf
        Map<String, String> udafs = Maps.newHashMap();
        for (Entry<String, Class<?>> entry : MeasureTypeFactory.getUDAFs().entrySet()) {
            udafs.put(entry.getKey(), entry.getValue().getName());
        }
        List<JsonFunction> udafJsonFunctionList = addToJsonFunctionList(udafs, false);
        jsonFunctionList.addAll(udafJsonFunctionList);

        String jsonFunc = JsonUtil.writeValueAsString(jsonFunctionList);
        out.append(" " + jsonFunc + " ");
    }

    public static List<JsonFunction> addToJsonFunctionList(Map<String, String> u, boolean isUDF) {
        List<JsonFunction> jsonFunctionList = new ArrayList<>();
        for (Map.Entry<String, String> e : u.entrySet()) {
            JsonFunction jsonFunction = new JsonFunction();
            jsonFunction.name = e.getKey().trim().toUpperCase();
            jsonFunction.className = e.getValue().trim();
            if (isUDF) {
                jsonFunction.methodName = "*";
            }
            jsonFunctionList.add(jsonFunction);
        }
        return jsonFunctionList;
    }
}
