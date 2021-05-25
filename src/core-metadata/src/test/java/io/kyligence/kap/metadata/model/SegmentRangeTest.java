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
package io.kyligence.kap.metadata.model;

import io.kyligence.kap.common.util.NLocalFileMetadataTestCase;
import lombok.val;
import org.apache.kylin.metadata.model.SegmentRange;
import org.junit.Assert;
import org.junit.Test;

public class SegmentRangeTest extends NLocalFileMetadataTestCase {

    @Test
    public void testKafkaOffsetRangeContains() {
        val rangeToMerge = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957130000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 500L));
        val seg1 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957130000L, 1613957140000L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 400L));
        val seg2 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957140000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 400L), createKafkaPartitionsOffset(3, 500L));
        Assert.assertTrue(rangeToMerge.contains(seg1));
        Assert.assertTrue(rangeToMerge.contains(seg2));
    }

    @Test
    public void testKafkaOffsetRangeEquals() {
        val seg1 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957130000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 500L));
        val seg2 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957130000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 500L));
        Assert.assertEquals(seg1, seg2);
    }

    @Test
    public void testKafkaOffsetRangeCompareTo() {
        val seg1 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957130000L, 1613957140000L,
                createKafkaPartitionsOffset(3, 300L), createKafkaPartitionsOffset(3, 400L));
        val seg2 = new SegmentRange.KafkaOffsetPartitionedSegmentRange(1613957140000L, 1613957150000L,
                createKafkaPartitionsOffset(3, 400L), createKafkaPartitionsOffset(3, 500L));
        Assert.assertTrue(seg1.compareTo(seg2) < 0);
        Assert.assertTrue(seg2.compareTo(seg1) > 0);
        Assert.assertTrue(seg1.compareTo(seg1) == 0);
    }
}
