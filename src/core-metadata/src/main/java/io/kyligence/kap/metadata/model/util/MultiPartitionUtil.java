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

package io.kyligence.kap.metadata.model.util;

import java.util.List;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.util.Pair;

import com.google.common.collect.Lists;

public class MultiPartitionUtil {

    public static boolean isSameValue(String[] values1, String[] values2) {
        if (values1.length != values2.length) {
            return false;
        }
        for (int i = 0; i < values1.length; i++) {
            if (!values1[i].equals(values2[i])) {
                return false;
            }
        }
        return true;
    }

    private static Pair<List<String[]>, List<String[]>> partitionValues(List<String[]> originValues,
            List<String[]> addValues) {
        List<String[]> oldValues = cloneList(originValues);
        List<String[]> newValues = cloneList(addValues);

        List<String[]> duplicates = Lists.newArrayList();
        List<String[]> absents = Lists.newArrayList();
        for (String[] newValue : newValues) {
            boolean isSame = false;
            for (String[] oldValue : oldValues) {
                isSame = isSameValue(oldValue, newValue);
                if (isSame) {
                    duplicates.add(newValue);
                    break;
                }
            }
            if (!isSame) {
                absents.add(newValue);
            }
        }
        return Pair.newPair(duplicates, absents);
    }

    public static List<String[]> findDuplicateValues(List<String[]> originValues, List<String[]> addValues) {
        return partitionValues(originValues, addValues).getFirst();
    }

    public static List<String[]> findAbsentValues(List<String[]> originValues, List<String[]> addValues) {
        return partitionValues(originValues, addValues).getSecond();
    }

    public static List<String[]> cloneList(List<String[]> list) {
        List<String[]> result = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(list)) {
            list.forEach(arr -> result.add(arr.clone()));
        }
        return result;
    }
}
