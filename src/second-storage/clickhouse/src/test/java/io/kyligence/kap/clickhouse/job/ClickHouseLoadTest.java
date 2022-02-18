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

import com.google.common.collect.ImmutableMap;
import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Map;

public class ClickHouseLoadTest {

    @Test
    public void getSizeOrderShardsTest() throws NoSuchMethodException, InvocationTargetException, IllegalAccessException {
        ClickHouseLoad clickHouseLoad = new ClickHouseLoad();

        Method getIndexInGroup = clickHouseLoad.getClass().getDeclaredMethod("getIndexInGroup", String[].class, Map.class);
        getIndexInGroup.setAccessible(true);
        Method orderGroupByIndex = clickHouseLoad.getClass().getDeclaredMethod("orderGroupByIndex", String[].class, int[].class);
        orderGroupByIndex.setAccessible(true);

        String node11 = "node11";
        String node21 = "node21";
        String node31 = "node31";

        int[] result1 = (int[]) getIndexInGroup.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                ImmutableMap.of(node11, 10L, node21, 15L, node31, 16L));
        String[] result11 = (String[]) orderGroupByIndex.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                result1);
        Assert.assertArrayEquals(result1, new int[]{0, 1, 2});
        Assert.assertArrayEquals(result11, new String[]{node11, node21, node31});

        int[] result2 = (int[]) getIndexInGroup.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                ImmutableMap.of(node11, 16L, node21, 15L, node31, 17L));
        String[] result22 = (String[]) orderGroupByIndex.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                result2);
        Assert.assertArrayEquals(result2, new int[]{1, 0, 2});
        Assert.assertArrayEquals(result22, new String[]{node21, node11, node31});

        int[] result3 = (int[]) getIndexInGroup.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                ImmutableMap.of(node11, 18L, node21, 15L, node31, 10L));
        String[] result33 = (String[]) orderGroupByIndex.invoke(
                clickHouseLoad,
                new String[]{node11, node21, node31},
                result3);
        Assert.assertArrayEquals(result3, new int[]{2, 1, 0});
        Assert.assertArrayEquals(result33, new String[]{node31, node21, node11});
    }
}
