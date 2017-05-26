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

package io.kyligence.kap.metadata.datatype;

import java.util.List;

import io.kyligence.kap.hbase.orderedbytes.OrderedNumeric;
import org.junit.Test;

import com.google.common.collect.Lists;

public class OrderedNumericSizeEstimator {
    @Test
    public void estimate() {
        OrderedNumeric ascending = OrderedNumeric.ASCENDING;
        List<Number> sampleData = Lists.newArrayList();
        sampleData.add(null);
        sampleData.add(0);

        sampleData.add(0.3);
        sampleData.add(0.93);
        sampleData.add(0.003241);
        sampleData.add(1.5);
        sampleData.add(1.90);
        sampleData.add(31.90);
        sampleData.add(9931.90);
        sampleData.add(9931.9332);
        sampleData.add(1209931.9332);
        sampleData.add(99991209931.9332);
        sampleData.add(-0.3);
        sampleData.add(-0.93);
        sampleData.add(-0.003241);
        sampleData.add(-1.5);
        sampleData.add(-1.90);
        sampleData.add(-31.90);
        sampleData.add(-9931.90);
        sampleData.add(-9931.9332);
        sampleData.add(-1209931.9332);
        sampleData.add(-99991209931.9332);

        int totalLength = 0;
        for (Number number : sampleData) {
            int i = ascending.encodedLength(number);
            System.out.println("Number " + number + " length is : " + i);

            totalLength += i;
        }
        System.out.println("AVG: " + (totalLength / sampleData.size()));
    }
}
