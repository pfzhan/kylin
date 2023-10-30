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

package org.apache.kylin.engine.spark.filter;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.HadoopUtil;
import org.apache.kylin.common.util.JsonUtil;
import org.apache.kylin.guava30.shaded.common.collect.Lists;
import org.apache.kylin.guava30.shaded.common.collect.Maps;
import org.apache.spark.sql.DataFrameWriter;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

public class ParquetBloomFilter {

    public static final Logger LOGGER = LoggerFactory.getLogger(ParquetBloomFilter.class);

    private static final SortedSet<ColumnFilter> columnFilters = new TreeSet<>();
    private static boolean loaded = false;
    private static final List<String> buildBloomColumns = Lists.newArrayList();

    public static void registerBloomColumnIfNeed(String project, String modelId) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (!config.isBloomBuildEnabled()) {
            return;
        }
        if (StringUtils.isBlank(project) || StringUtils.isBlank(modelId)) {
            // won't happen
            return;
        }
        if (loaded) {
            return;
        }
        try {
            project = StringUtils.upperCase(project);
            FileSystem fs = HadoopUtil.getFileSystem(config.getHdfsWorkingDirectory());
            Path filterInfo = new Path(QueryFiltersCollector.FILTER_STORAGE_PATH);
            if (!fs.exists(filterInfo)) {
                loaded = true;
                return;
            }
            FileStatus[] hostsDir = fs.listStatus(new Path(QueryFiltersCollector.FILTER_STORAGE_PATH));
            HashMap<String, Integer> columnsHits = Maps.newHashMap();
            for (FileStatus host : hostsDir) {
                String hostName = host.getPath().getName();
                Path projectFiltersFile = QueryFiltersCollector.getProjectFiltersFile(hostName, project);
                if (!fs.exists(projectFiltersFile)) {
                    continue;
                }
                Map<String, Map<String, Integer>> modelColumns = JsonUtil
                        .readValue(HadoopUtil.readStringFromHdfs(fs, projectFiltersFile), Map.class);
                if (modelColumns.containsKey(modelId)) {
                    modelColumns.get(modelId).forEach((column, hit) -> {
                        int originHit = columnsHits.getOrDefault(column, 0);
                        columnsHits.put(column, originHit + hit);
                    });
                }
            }
            columnsHits.forEach((column, hit) -> columnFilters.add(new ColumnFilter(column, hit)));
            String columnFiltersLog = Arrays.toString(columnFilters.toArray());
            LOGGER.info("Register BloomFilter info from HDFS: {}", columnFiltersLog);
        } catch (Exception e) {
            LOGGER.error("Error when register BloomFilter.", e);
        }
        loaded = true;
    }

    public static void configBloomColumnIfNeed(Dataset<Row> data, DataFrameWriter<Row> dataWriter) {
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        if (!config.isBloomBuildEnabled()) {
            return;
        }
        String manualColumn = config.getBloomBuildColumn();
        String bloomColumnIds = config.getBloomBuildColumnIds();
        if (StringUtils.isNotBlank(manualColumn)) {
            String[] blooms = manualColumn.split("#");
            for (int i = 0; i < blooms.length; i += 2) {
                String nvd = blooms[i + 1];
                dataWriter.option("parquet.bloom.filter.enabled#" + blooms[i], "true");
                dataWriter.option("parquet.bloom.filter.expected.ndv#" + blooms[i], nvd);
                LOGGER.info("build BloomFilter info: columnId is {}, nvd is {}", blooms[i], nvd);
                buildBloomColumns.add(blooms[i]);
            }
            return;
        }
        dataWriter.option("parquet.bloom.filter.dynamic.enabled", "true");
        dataWriter.option("parquet.bloom.filter.candidate.size", "10");
        if (StringUtils.isNotBlank(bloomColumnIds)) {
            String[] blooms = bloomColumnIds.split("#");
            for (int i = 0; i < blooms.length; i++) {
                dataWriter.option("parquet.bloom.filter.enabled#" + blooms[i], "true");
                LOGGER.info("build dynamic BloomFilter info: columnIds is {}", blooms[i]);
                buildBloomColumns.add(blooms[i]);
            }
            return;
        }
        Set<String> columns = Arrays.stream(data.columns()).collect(Collectors.toSet());
        Set<ColumnFilter> dataColumns = columnFilters.stream().filter(column -> columns.contains(column.columnId))
                .collect(Collectors.toSet());
        int count = 0;
        for (ColumnFilter columnFilter : dataColumns) {
            if (count >= config.getBloomBuildColumnMaxNum()) {
                break;
            }
            dataWriter.option("parquet.bloom.filter.enabled#" + columnFilter.columnId, "true");
            buildBloomColumns.add(columnFilter.columnId);
            LOGGER.info("building BloomFilter : columnId is {}", columnFilter.columnId);
            count++;
        }
    }

    private ParquetBloomFilter() {
    }

    // Only for Unit Test
    public static void resetParquetBloomFilter() {
        ParquetBloomFilter.loaded = false;
        ParquetBloomFilter.buildBloomColumns.clear();
        ParquetBloomFilter.columnFilters.clear();
    }

    public static List<String> getBuildBloomColumns() {
        return buildBloomColumns;
    }

    public static boolean isLoaded() {
        return loaded;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ColumnFilter implements Comparable<ColumnFilter> {
        private String columnId;
        private int hit;

        @Override
        public int compareTo(ColumnFilter o) {
            if (o.hit != this.hit) {
                return Integer.compare(o.hit, this.hit);
            }
            return o.columnId.compareTo(this.columnId);
        }
    }
}
