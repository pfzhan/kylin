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

package io.kyligence.kap.query.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.apache.kylin.common.util.Pair;

public class KapRelUtil {

    private KapRelUtil() {
    }

    public static String getDigestWithoutRelNodeId(String digest, long layoutId, String modelId) {
        StringBuilder digestWithoutId = new StringBuilder();
        boolean isPointToId = false;
        for (char c : digest.toCharArray()) {
            if (isPointToId && !isCharNum(c)) {
                // end point to id
                isPointToId = false;
            }
            if (isPointToId) {
                continue;
            }
            if (c == '#') {
                // start point to id
                isPointToId = true;
            }
            digestWithoutId.append(c);
        }
        return replaceDigestCtxValueByLayoutIdAndModelId(digestWithoutId.toString(), layoutId, modelId);
    }

    private final static String CTX = "ctx=";

    public static String replaceDigestCtxValueByLayoutIdAndModelId(String digestId, long layoutId, String modelId) {
        if (layoutId <= 0 || "".equals(modelId)) {
            return digestId;
        }
        StringBuilder digestBuilder = new StringBuilder();
        char[] digestArray = digestId.toCharArray();
        char[] compareArray = CTX.toCharArray();
        int len = digestArray.length;
        for (int i = 0; i < len; i++) {
            char c1 = digestArray[i];
            if (c1 == compareArray[0] && (i + 3) < len && digestArray[i + 1] == compareArray[1]
                    && digestArray[i + 2] == compareArray[2] && digestArray[i + 3] == compareArray[3]) {
                digestBuilder.append(CTX);
                i = i + 2;
                while (digestArray[i + 1] != ',' && digestArray[i + 1] != ')') {
                    i = i + 1;
                }
                digestBuilder.append(modelId + "_" + layoutId);
            } else {
                digestBuilder.append(c1);
            }
        }
        return digestBuilder.toString();
    }

    private static boolean isCharNum(char c) {
        return c >= '0' && c <= '9';
    }

    public static RexNode isNotDistinctFrom(RelNode left, RelNode right, RexNode condition,
                                            List<Pair<Integer, Integer>> pairs, List<Boolean> filterNulls) {
        final List<Integer> leftKeys = new ArrayList<>();
        final List<Integer> rightKeys = new ArrayList<>();
        RexNode rexNode = RelOptUtil.splitJoinCondition(left, right, condition, leftKeys, rightKeys, filterNulls);
        for (int i = 0; i < leftKeys.size(); i++) {
            pairs.add(new Pair<>(leftKeys.get(i), rightKeys.get(i) + left.getRowType().getFieldCount()));
        }
        return rexNode;
    }
}
