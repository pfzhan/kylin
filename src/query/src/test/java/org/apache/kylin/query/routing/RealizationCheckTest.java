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

package org.apache.kylin.query.routing;

import com.google.common.collect.Lists;
import io.kyligence.kap.metadata.model.NDataModel;
import org.apache.kylin.metadata.model.TblColRef;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class RealizationCheckTest {

    @Test
    public void testRealizationCheck() {
        RealizationCheck realizationCheck = new RealizationCheck();
        NDataModel dataModel = Mockito.mock(NDataModel.class);
        Mockito.when(dataModel.findColumn("LO_DATE")).thenReturn(Mockito.mock(TblColRef.class));

        realizationCheck.addModelIncapableReason(dataModel,
                RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_CONTAIN_ALL_COLUMN));
        realizationCheck.addModelIncapableReason(dataModel,
                RealizationCheck.IncapableReason.create(RealizationCheck.IncapableType.CUBE_NOT_CONTAIN_ALL_MEASURE));
        realizationCheck.addModelIncapableReason(dataModel,
                RealizationCheck.IncapableReason.notContainAllColumn(Lists.<TblColRef>newArrayList()));
        realizationCheck.addModelIncapableReason(dataModel,
                RealizationCheck.IncapableReason.notContainAllColumn(Lists.<TblColRef>newArrayList()));
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().size() == 1);
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().get(dataModel).size() == 3);

        realizationCheck.addModelIncapableReason(dataModel, RealizationCheck.IncapableReason
                .notContainAllColumn(Lists.<TblColRef>newArrayList(dataModel.findColumn("LO_DATE"))));
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().size() == 1);
        Assert.assertTrue(realizationCheck.getModelIncapableReasons().get(dataModel).size() == 4);
    }
}
