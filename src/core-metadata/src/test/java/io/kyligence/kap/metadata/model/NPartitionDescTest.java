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

import org.apache.kylin.metadata.model.PartitionDesc;
import org.junit.Assert;
import org.junit.Test;

public class NPartitionDescTest {
    private static final String DATE_COL = "dateCol";
    private static final String DATE_FORMAT = "dateFormat";
    private static final String TIME_FORMAT = "timeFormat";
    private static final String TIME_COL = "timeCol";
    PartitionDesc.PartitionType partitionType = PartitionDesc.PartitionType.APPEND;
    @Test
    public void testEquals() {
        PartitionDesc partitionDesc1 = new PartitionDesc();
        Assert.assertEquals(partitionDesc1, partitionDesc1);
        Assert.assertNotEquals(partitionDesc1, new Integer(1));
        Assert.assertNotEquals(partitionDesc1, null);

        partitionDesc1.setCubePartitionType(partitionType);
        partitionDesc1.setPartitionDateColumn(DATE_COL);
        partitionDesc1.setPartitionTimeColumn(TIME_COL);
        partitionDesc1.setPartitionDateFormat(DATE_FORMAT);
        partitionDesc1.setPartitionTimeFormat(TIME_FORMAT);

        PartitionDesc partitionDesc2 = new PartitionDesc();
        partitionDesc2.setCubePartitionType(partitionType);
        partitionDesc2.setPartitionDateColumn(DATE_COL);
        partitionDesc2.setPartitionTimeColumn(TIME_COL);
        partitionDesc2.setPartitionDateFormat(DATE_FORMAT);
        partitionDesc2.setPartitionTimeFormat(TIME_FORMAT);
        Assert.assertEquals(partitionDesc1, partitionDesc2);
        Assert.assertEquals(partitionDesc1.hashCode(), partitionDesc2.hashCode());
        partitionDesc2.setPartitionDateFormat("new_date");
        Assert.assertNotEquals(partitionDesc1, partitionDesc2);

        PartitionDesc partitionDesc3 = PartitionDesc.getCopyOf(partitionDesc1);
        Assert.assertEquals(partitionDesc1.hashCode(), partitionDesc3.hashCode());
    }
}
