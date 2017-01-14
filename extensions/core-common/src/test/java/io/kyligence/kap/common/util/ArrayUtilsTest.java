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

package io.kyligence.kap.common.util;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertNull;

import java.util.List;

import org.junit.Test;

import com.google.common.collect.Lists;

public class ArrayUtilsTest {
    @Test
    public void testTo2DArray() {
        // normal case
        List<List<String>> input = Lists.newArrayList();
        input.add(Lists.newArrayList("1", "2"));
        input.add(Lists.newArrayList("3"));
        input.add(Lists.newArrayList("4", "5", "6"));
        input.add(null);
        input.add(Lists.<String> newArrayList());

        String[][] expected = new String[5][];
        expected[0] = new String[] { "1", "2" };
        expected[1] = new String[] { "3" };
        expected[2] = new String[] { "4", "5", "6" };
        expected[3] = null;
        expected[4] = new String[0];

        assertArrayEquals(expected, ArrayUtils.to2DArray(input));

        // empty case
        assertArrayEquals(new String[0][], ArrayUtils.to2DArray(Lists.<List<String>> newArrayList()));

        // null case
        assertNull(ArrayUtils.to2DArray(null));
    }
}
