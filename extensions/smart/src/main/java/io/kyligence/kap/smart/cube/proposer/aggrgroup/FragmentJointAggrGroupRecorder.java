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

package io.kyligence.kap.smart.cube.proposer.aggrgroup;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.smart.common.SmartConfig;

public class FragmentJointAggrGroupRecorder {
    private SmartConfig smartConfig;

    private static final Logger logger = LoggerFactory.getLogger(FragmentJointAggrGroupRecorder.class);
    private Map<String, Column> candidates = Maps.newHashMap();

    public FragmentJointAggrGroupRecorder(SmartConfig smartConfig) {
        this.smartConfig = smartConfig;
    }

    public void add(String col, double score) {
        candidates.put(col, new Column(col, score));
    }

    public List<List<String>> getResult(int scaleTimes, List<List<String>>... ignored) {
        if (ignored != null) {
            for (List<List<String>> l1 : ignored) {
                for (List<String> l2 : l1) {
                    for (String l3 : l2) {
                        candidates.remove(l3);
                    }
                }
            }
        }

        List<List<String>> result = Lists.newArrayList();
        if (scaleTimes <= smartConfig.getAggGroupStrictRetryMax() / 2) {
            result.addAll(groupByName()); // ignore name patterns if retried too many times.
        }
        result.addAll(groupRandom(scaleTimes));

        Preconditions.checkArgument(candidates.isEmpty());
        return result;
    }

    private List<List<String>> groupRandom(int scaleTimes) {
        // Candidates with different name pattern will form groups randomly, with the MAX of multiply of cardinality.
        // TODO: In future, we can use DP to find the optimized solution.
        List<List<String>> result = Lists.newArrayList();

        Iterator<Map.Entry<String, Column>> columnIter = candidates.entrySet().iterator();
        List<String> currGroup = Lists.newArrayList();
        double currScore = 1;
        while (columnIter.hasNext()) {
            Column column = columnIter.next().getValue();
            if (currScore * column.score > smartConfig.getJointGroupCardinalityMax() * Math.pow(10, scaleTimes) || currGroup.size() >= smartConfig.getJointColNumMax() * (scaleTimes / 3 + 1)) {
                if (currGroup.size() > 1) {
                    result.add(currGroup);
                }
                currGroup = Lists.newArrayList();
                currScore = 1;
            }
            currGroup.add(column.name);
            currScore *= column.score;

            columnIter.remove();
        }

        if (currGroup.size() > 1) {
            result.add(currGroup);
        }

        logger.trace("Added {} joint from merging small cardinality dimensions.", result.size());

        return result;
    }

    private List<List<String>> groupByName() {
        // In most cases, candidates with small cardinality have similar name pattern, such as IS_XXX or XXX_FLAG or XXX_TYPE,
        // and similar candidates often have relations and describe an entity together. In order to get more friendly suggestion,
        // we put similar candidates in one joint group in the first.
        Map<String, List<String>> prefixClustered = Maps.newHashMap();
        Map<String, List<String>> postfixClustered = Maps.newHashMap();

        Iterator<Map.Entry<String, Column>> columnIter = candidates.entrySet().iterator();
        while (columnIter.hasNext()) {
            Column column = columnIter.next().getValue();
            String columnName = column.name;
            String tblName = "";
            if (columnName.contains(".")) {
                String[] tmp = columnName.split("\\.");
                tblName = tmp[0].trim();
                columnName = tmp[1].trim();
            }

            String[] comps = columnName.split("_");
            if (comps.length > 1) {
                {
                    // prefix same
                    String prefix = comps[0];
                    String key = tblName + "|" + prefix;
                    List<String> prefixList = prefixClustered.get(key);
                    if (prefixList == null) {
                        prefixList = Lists.newArrayList();
                        prefixClustered.put(key, prefixList);
                    }
                    prefixList.add(column.name);
                }
                {
                    // post same
                    String postfix = comps[comps.length - 1];
                    String key = tblName + "|" + postfix;
                    List<String> postfixList = postfixClustered.get(key);
                    if (postfixList == null) {
                        postfixList = Lists.newArrayList();
                        postfixClustered.put(key, postfixList);
                    }
                    postfixList.add(column.name);
                }
                columnIter.remove();
            }
        }

        List<List<String>> result = Lists.newArrayList();
        Set<String> usedCols = Sets.newHashSet();

        for (Map.Entry<String, List<String>> prefixClusteredEntry : prefixClustered.entrySet()) {
            List<String> candidate = prefixClusteredEntry.getValue();
            if (candidate.size() > 1) {
                result.addAll(splitWithSizeLimit(candidate));
                usedCols.addAll(candidate);
            }
        }
        for (Map.Entry<String, List<String>> postfixClusteredEntry : postfixClustered.entrySet()) {
            List<String> candidate = postfixClusteredEntry.getValue();
            candidate.removeAll(usedCols);
            if (candidate.size() > 1) {
                result.addAll(splitWithSizeLimit(candidate));
                usedCols.addAll(candidate);
            }
        }

        logger.trace("Added {} joint from merging similar pattern dimensions.", result.size());

        return result;
    }

    private List<List<String>> splitWithSizeLimit(List<String> list) {
        List<List<String>> results = Lists.newArrayList();
        List<String> currentResult = Lists.newArrayList();
        for (int i = 0; i < list.size(); i++) {
            if (currentResult.size() >= smartConfig.getJointColNumMax()) {
                results.add(currentResult);
                currentResult = Lists.newArrayList();
            }
            currentResult.add(list.get(i));
        }

        if (currentResult.size() > 1) {
            results.add(currentResult);
        }

        return results;
    }

    private class Column {
        String name;
        double score;

        private Column(String name, double score) {
            this.name = name;
            this.score = score;
        }
    }
}
