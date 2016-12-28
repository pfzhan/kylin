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

package io.kyligence.kap.measure.percentile;

import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.Random;

import org.apache.kylin.metadata.datatype.DataType;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import io.kyligence.kap.common.util.LocalFileMetadataTestCase;

/**
 * Created by dongli on 5/21/16.
 */
public class PercentileSerializerTest extends LocalFileMetadataTestCase {

    @Before
    public void setup() throws Exception {
        createTestMetadata();
    }

    @After
    public void after() throws Exception {
        cleanAfterClass();
    }

    @Test
    public void testBasic() {
        PercentileSerializer serializer = new PercentileSerializer(DataType.getType("percentile(100)"));
        PercentileCounter counter = new PercentileCounter(100, 0.5);
        Random random = new Random();
        for (int i = 0; i < 1000; i++) {
            counter.add(random.nextDouble());
        }
        double markResult = counter.getResultEstimate();

        ByteBuffer buffer = ByteBuffer.allocateDirect(serializer.getStorageBytesEstimate());
        serializer.serialize(counter, buffer);

        buffer.flip();
        counter = serializer.deserialize(buffer);
        PercentileCounter counter1 = new PercentileCounter(100, 0.5);
        counter1.merge(counter);

        assertEquals(markResult, counter1.getResultEstimate(), 0.01);
    }

}
