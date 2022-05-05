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

package io.kyligence.kap.clickhouse.job;

import com.google.common.base.Preconditions;
import io.kyligence.kap.secondstorage.SecondStorageNodeHelper;
import io.kyligence.kap.secondstorage.SecondStorageQueryRouteUtil;
import io.kyligence.kap.secondstorage.util.SecondStorageDateUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.kylin.metadata.model.SegmentRange;

import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ClickHouseTableStorageMetric {
    private List<String> nodes;
    private boolean load = false;
    private Map<String, List<ClickHouseSystemQuery.PartitionSize>> partitionSize;

    public ClickHouseTableStorageMetric(List<String> nodes) {
        this.nodes = nodes;
    }

    public void collect(boolean skipDownNode) {
        if (load) return;
        partitionSize = nodes.parallelStream().collect(Collectors.toMap(node -> node,
                node -> {
                    if (skipDownNode && !SecondStorageQueryRouteUtil.getNodeStatus(node)) {
                        return Collections.emptyList();
                    }
                    try (ClickHouse clickHouse = new ClickHouse(SecondStorageNodeHelper.resolve(node))) {
                        return clickHouse.query(ClickHouseSystemQuery.queryTableStorageSize(), ClickHouseSystemQuery.TABLE_STORAGE_MAPPER);
                    } catch (SQLException sqlException) {
                        return ExceptionUtils.rethrow(sqlException);
                    }
                }));
        load = true;
    }

    public Map<String, Long> getByPartitions(String database, String table, SegmentRange segmentRange, String dateFormat) {
        Map<String, Long> sizeInNode;
        if (segmentRange.isInfinite()) {
            sizeInNode = this.getByPartitions(database, table, Collections.singletonList("tuple()"));
        } else {
            SimpleDateFormat partitionFormat = new SimpleDateFormat(dateFormat, Locale.getDefault(Locale.Category.FORMAT));
            sizeInNode = this.getByPartitions(database, table,
                    SecondStorageDateUtils.splitByDay((SegmentRange<Long>) segmentRange).stream()
                            .map(partitionFormat::format).collect(Collectors.toList()));
        }
        return sizeInNode;
    }

    public Map<String, Long> getByPartitions(String database, String table, List<String> partitions) {
        Preconditions.checkArgument(load);
        Set<String> partitionSet = new HashSet<>(partitions);
        return nodes.stream().collect(Collectors.toMap(node -> node,
                node -> sumNodeTableSize(node, database, table, partitionSet)));
    }

    private long sumNodeTableSize(String node, String database, String table, Set<String> partitionSet) {
        return partitionSet.stream()
                .flatMap(partition -> partitionSize.get(node).stream()
                        .filter(size -> Objects.equals(database, size.getDatabase())
                                && Objects.equals(table, size.getTable())
                                && Objects.equals(partition, size.getPartition()))
                        .map(ClickHouseSystemQuery.PartitionSize::getBytes))
                .reduce(Long::sum).orElse(0L);
    }
}
