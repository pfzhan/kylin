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

package io.kyligence.kap.smart.query;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import lombok.val;
import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.metadata.model.FunctionDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.model.NDataModel;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE, isGetterVisibility = JsonAutoDetect.Visibility.NONE, setterVisibility = JsonAutoDetect.Visibility.NONE)
public class QueryStats implements Serializable {
    @JsonProperty("total_queries")
    private int totalQueries;

    @JsonProperty("group_by")
    private Map<String, Integer> groupBys = Maps.newHashMap();

    @JsonProperty("filter")
    private Map<String, Integer> filters = Maps.newHashMap();

    @JsonProperty("cuboids")
    private Set<Set<String>> cuboids = Sets.newHashSet();

    @JsonProperty("appear")
    private Map<String, Integer> appears = Maps.newHashMap();

    @JsonProperty("measure")
    private List<FunctionDesc> measures = Lists.newArrayList();

    @JsonProperty("coocurrence")
    private Map<String, Integer> coocurrences = Maps.newHashMap();

    public static QueryStats merge(List<QueryStats> queryStats) {
        QueryStats result = new QueryStats();
        for (QueryStats stats : queryStats) {
            result.merge(stats);
        }
        return result;
    }

    public static QueryStats buildFromOLAPContext(final OLAPContext ctx, final NDataModel model,
            Map<String, String> aliasMatch) {
        QueryStats r = new QueryStats();

        ctx.fixModel(model, aliasMatch);
        r.addTotalQueries();
        r.addGroupBy(Collections2.transform(ctx.getGroupByColumns(), new Function<TblColRef, String>() {
            @Nullable
            @Override
            public String apply(@Nullable TblColRef input) {
                return input.getIdentity();
            }
        }));

        r.addFilter(Collections2.transform(ctx.filterColumns, new Function<TblColRef, String>() {
            @Nullable
            @Override
            public String apply(@Nullable TblColRef input) {
                return input.getIdentity();
            }
        }));

        Collection<String> allCols = Collections2
                .transform(Collections2.filter(ctx.allColumns, new Predicate<TblColRef>() {
                    @Override
                    public boolean apply(@Nullable TblColRef input) {
                        if (!model.getAliasMap().containsKey(input.getTableAlias()))
                            return false;

                        if (ctx.metricsColumns.contains(input) && !ctx.getGroupByColumns().contains(input)
                                && !ctx.filterColumns.contains(input))
                            return false;

                        return true;
                    }
                }), new Function<TblColRef, String>() {
                    @Nullable
                    @Override
                    public String apply(@Nullable TblColRef input) {
                        return input.getIdentity();
                    }
                });

        Collection<FunctionDesc> measures = Lists.newArrayList();
        for (FunctionDesc aggFunc : ctx.aggregations) {
            val params = aggFunc.getParameters();
            final List<ParameterDesc> copyParams = Lists.newArrayList();

            if (CollectionUtils.isNotEmpty(params)) {
                if (params.get(0).isColumnType()) {
                    if (!params.get(0).getColRef().isQualified())
                        continue;

                    params.stream().filter(ParameterDesc::isColumnType)
                            .forEach(param -> copyParams.add(ParameterDesc.newInstance(param.getColRef())));
                } else {
                    copyParams.add(ParameterDesc.newInstance(params.get(0).getValue()));
                }
            }

            FunctionDesc copy = FunctionDesc.newInstance(aggFunc.getExpression(), copyParams, aggFunc.getReturnType());
            if (copy.getReturnType() == null && copy.getExpression().equals("SUM")) {
                copy.setReturnType("decimal(19,4)"); //TODO: need to figure out why return_type missing.
            }
            measures.add(copy);
        }
        r.addAppear(allCols);
        r.addColPairs(allCols);
        r.addCuboidCols(Sets.newHashSet(allCols));
        r.addMeasures(measures);
        ctx.unfixModel();

        return r;
    }

    public int getTotalQueries() {
        return totalQueries;
    }

    public void addTotalQueries() {
        totalQueries++;
    }

    public void addColPairs(Collection<String> colNames) {
        String[] colNameArr = Iterables.toArray(colNames, String.class);
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
        }
    }

    public void addFilter(Collection<String> filterCols) {
        for (String filterCol : filterCols) {
            incMapVal(filters, filterCol);
        }
    }

    public void addAppear(Collection<String> appearCols) {
        for (String appearCol : appearCols) {
            incMapVal(appears, appearCol);
        }
    }

    public void addCuboidCols(Set<String> cols) {
        cuboids.add(cols);
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

    public void addMeasures(Collection<FunctionDesc> measureFuncs) {
        measures.addAll(measureFuncs);
    }

    public void addMeasure(FunctionDesc measureFunc) {
        measures.add(measureFunc);
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

    public Set<Set<String>> getCuboids() {
        return cuboids;
    }

    public List<FunctionDesc> getMeasures() {
        return measures;
    }

    public Map<String, Integer> getCoocurrences() {
        return coocurrences;
    }

    public void merge(QueryStats other) {
        if (other == null)
            return;

        totalQueries += other.getTotalQueries();
        cuboids.addAll(other.cuboids);
        measures.addAll(other.measures);

        mergeMapVals(groupBys, other.groupBys);
        mergeMapVals(filters, other.filters);
        mergeMapVals(appears, other.appears);
        mergeMapVals(coocurrences, other.coocurrences);
    }

    private void incMapVal(Map<String, Integer> map, String key) {
        Integer val = map.get(key);
        if (val == null) {
            val = 0;
        }
        map.put(key, val + 1);
    }

    private void mergeMapVals(Map<String, Integer> self, Map<String, Integer> other) {
        for (Map.Entry<String, Integer> entry : other.entrySet()) {
            Integer val = self.get(entry.getKey());
            if (val == null)
                val = 0;
            self.put(entry.getKey(), val + entry.getValue());
        }
    }

    private String createCoocurrenceKey(String col1, String col2) {
        int compare = col1.compareTo(col2);
        if (compare > 0) {
            return String.format("%s,%s", col1, col2);
        } else {
            return String.format("%s,%s", col2, col1);
        }
    }
}
