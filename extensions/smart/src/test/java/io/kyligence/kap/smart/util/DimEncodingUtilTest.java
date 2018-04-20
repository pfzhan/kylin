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

package io.kyligence.kap.smart.util;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DimEncodingUtilTest {

    private static List<long[]> successValue;

    @BeforeClass
    public static void initTestValue() {
        successValue = new ArrayList<>();
        successValue.add(new long[] { -127, 0, 100, -100, 23, 32, 127 });
        successValue.add(new long[] { -32767, 256, 200, -300, -10000, 20000, 32767 });
        successValue.add(new long[] { -8388607, 40000, -50000, 100000, 8388607 });
        successValue.add(new long[] { -2147483647L, 8388699L, 2147483647L });
        successValue.add(new long[] { -549755813887L, 2147483699L, 549755813887L });
        successValue.add(new long[] { -140737488355327L, 549755813899L, 140737488355327L });
        successValue.add(new long[] { -36028797018963967L, 140737488359927L, 36028797018963967L });
        successValue.add(new long[] { -9223372036854775807L, 36099797018963967L, 9223372036854775807L });
    }

    @Test
    public void testGetIntEncodingLength() {
        Assert.assertEquals(1, DimEncodingUtil.getIntEncodingLength((0)));
        for (int i = 0; i < successValue.size(); i++) {
            for (long v : successValue.get(i)) {
                assertEquals(i + 1, DimEncodingUtil.getIntEncodingLength(v));
            }
        }
    }
}
