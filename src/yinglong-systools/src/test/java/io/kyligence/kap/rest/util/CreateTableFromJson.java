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

package io.kyligence.kap.rest.util;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * A tool to generate database and tables from metadata backups of customer
 */
public class CreateTableFromJson {

    public static void main(String[] args) throws IOException {

        String pathDir = args[0];
        Map<String, List<String>> map = createDbAndTables(pathDir);

        map.forEach((k, v) -> {
            System.out.println(k);
            v.forEach(System.out::println);
        });

        System.out.println("\n\n\n\n\n");
    }

    // the path is /{metadata_backup_path}/{project_name}/table/
    private static Map<String, List<String>> createDbAndTables(String pathDir) throws IOException {
        Map<String, List<String>> map = Maps.newHashMap();
        File file = new File(pathDir).getAbsoluteFile();
        File[] files = file.listFiles();

        for (File f : Objects.requireNonNull(files)) {
            final TableDesc tableDesc = JsonUtil.readValue(f, TableDesc.class);
            List<String> columnNameTypeList = Lists.newArrayList();
            for (ColumnDesc column : tableDesc.getColumns()) {
                String name = column.getName();
                String type = convert(column.getDatatype());
                columnNameTypeList.add(String.format(Locale.ROOT, "%s %s", quote(name), type));
            }

            String databaseSql = String.format(Locale.ROOT, "create database %s;\nuse %s;", quote(tableDesc.getDatabase()),
                    quote(tableDesc.getDatabase()));
            map.putIfAbsent(databaseSql, Lists.newArrayList());
            String tableSql = createTableSql(tableDesc.getName(), columnNameTypeList);
            map.get(databaseSql).add(tableSql);
        }
        return map;
    }

    private static final String QUOTE = "`";
    private static final Map<String, String> TYPE_MAP = Maps.newHashMap();
    static {
        TYPE_MAP.put("integer", "int");
        TYPE_MAP.put("long", "bigint");
    }

    private static String quote(String identifier) {
        return QUOTE + identifier + QUOTE;
    }

    private static String createTableSql(String table, List<String> columns) {
        return String.format(Locale.ROOT, "create table %s(%s);", quote(table), String.join(", ", columns));
    }

    private static String convert(String oriType) {
        return TYPE_MAP.getOrDefault(oriType.toLowerCase(Locale.ROOT), oriType.toLowerCase(Locale.ROOT));
    }
}
