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

package io.kyligence.kap.modeling.smart.query;

import java.io.Serializable;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

import org.apache.kylin.metadata.model.FunctionDesc;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class QueryStats implements Serializable {
    @JsonProperty("total_queries")
    private int totalQueries;

    @JsonProperty("column_bitmap")
    private long columnBitmap;

    @JsonProperty("group_by")
    private Map<String, Integer> groupBys = Maps.newHashMap();

    @JsonProperty("filter")
    private Map<String, Integer> filters = Maps.newHashMap();

    @JsonProperty("appear")
    private Map<String, Integer> appears = Maps.newHashMap();

    @JsonProperty("measure")
    private Set<FunctionDesc> measures = Sets.newHashSet();

    @JsonProperty("coocurrence")
    private Map<String, Integer> coocurrences = Maps.newHashMap();

    public int getTotalQueries() {
        return totalQueries;
    }

    public void addTotalQueries() {
        totalQueries++;
    }

    private String createCoocurrenceKey(String col1, String col2) {
        int compare = col1.compareTo(col2);
        if (compare > 0) {
            return String.format("%s,%s", col1, col2);
        } else {
            return String.format("%s,%s", col2, col1);
        }
    }

    public void addColPairs(Collection<String>... cols) {
        Set<String> colNames = Sets.newHashSet();
        for (Collection<String> col : cols) {
            colNames.addAll(col);
        }

        String[] colNameArr = colNames.toArray(new String[0]);
        for (int i = 0; i < colNames.size(); i++) {
            for (int j = i + 1; j < colNames.size(); j++) {
                String key = createCoocurrenceKey(colNameArr[i], colNameArr[j]);
                int val = 0;
                if (coocurrences.containsKey(key)) {
                    val = coocurrences.get(key);
                }
                coocurrences.put(key, val + 1);
            }
        }
    }

    public void addGroupBy(Collection<String> groupByCols) {
        for (String groupByCol : groupByCols) {
            incMapVal(groupBys, groupByCol);
            incMapVal(appears, groupByCol);
        }
    }

    public void addFilter(Collection<String> filterCols) {
        for (String filterCol : filterCols) {
            incMapVal(filters, filterCol);
            incMapVal(appears, filterCol);
        }
    }

    public void putOneColumn(String colName, int filter, int groupBy, int appear) {
        filters.put(colName, filter);
        groupBys.put(colName, groupBy);
        appears.put(colName, appear);
    }

    public void putPairColumn(String col1, String col2, int num) {
        String key = createCoocurrenceKey(col1, col2);
        coocurrences.put(key, num);
    }

    private void incMapVal(Map<String, Integer> map, String key) {
        Integer val = map.get(key);
        if (val == null) {
            val = 0;
        }
        map.put(key, val + 1);
    }

    public void addMeasures(Collection<FunctionDesc> measureFuncs) {
        for (FunctionDesc measureFunc : measureFuncs) {
            measures.add(measureFunc);
        }
    }

    public void addMeasure(FunctionDesc measureFunc) {
        measures.add(measureFunc);
    }

    public void addCuboid(long cuboidId) {
        columnBitmap |= cuboidId;
    }

    public long getColumnBitmap() {
        return columnBitmap;
    }

    public Map<String, Integer> getGroupBys() {
        return groupBys;
    }

    public Map<String, Integer> getFilters() {
        return filters;
    }

    public Map<String, Integer> getAppears() {
        return appears;
    }

    public Set<FunctionDesc> getMeasures() {
        return measures;
    }

    public Map<String, Integer> getCoocurrences() {
        return coocurrences;
    }
}
