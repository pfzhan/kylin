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

package io.kyligence.kap.cube.mp;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.CubeSegment;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.PartitionDesc.IPartitionConditionBuilder;
import org.apache.kylin.metadata.model.SegmentRange;
import org.apache.kylin.metadata.model.TblColRef;

import io.kyligence.kap.metadata.model.KapModel;

public class MPSqlCondBuilder implements IPartitionConditionBuilder {

    @Override
    public String buildDateRangeCondition(PartitionDesc partDesc, ISegment seg, SegmentRange segRange) {
        IPartitionConditionBuilder dft = new PartitionDesc.DefaultPartitionConditionBuilder();
        String cond = dft.buildDateRangeCondition(partDesc, seg, segRange);

        KapModel model = (KapModel) seg.getModel();
        if (null == model) {
            return cond;
        }

        if (model.isMultiLevelPartitioned() == false)
            throw new IllegalStateException();

        StringBuilder buf = new StringBuilder();
        if (!StringUtils.isBlank(cond))
            buf.append("(" + cond + ")");

        CubeInstance cube = ((CubeSegment) seg).getCubeInstance();
        String[] mpValues = MPCubeManager.getInstance(model.getConfig()).fetchMPValues(cube);
        TblColRef[] mpCols = model.getMutiLevelPartitionCols();

        for (int i = 0; i < mpCols.length; i++) {
            if (buf.length() > 0)
                buf.append(" and ");

            TblColRef c = mpCols[i];
            buf.append(c.getIdentity());
            buf.append("=");
            if (c.getType().isNumberFamily())
                buf.append(mpValues[i]);
            else
                buf.append("'" + mpValues[i] + "'");
        }
        return buf.toString();
    }
}
