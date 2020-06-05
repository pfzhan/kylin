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

package io.kyligence.kap.metadata.recommendation.v2;

import com.google.common.base.Preconditions;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.DimensionRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.MeasureRecItemV2;

public class RecommendationUtil {

    public static ComputedColumnDesc getCC(RawRecItem rawRecItem) {
        Preconditions.checkNotNull(rawRecItem);
        Preconditions.checkState(RawRecItem.RawRecType.COMPUTED_COLUMN == rawRecItem.getType());
        CCRecItemV2 recItemV2 = (CCRecItemV2) rawRecItem.getRecEntity();
        return recItemV2.getCc();
    }

    public static NDataModel.Measure getMeasure(RawRecItem rawRecItem) {
        Preconditions.checkNotNull(rawRecItem);
        Preconditions.checkState(RawRecItem.RawRecType.MEASURE == rawRecItem.getType());
        MeasureRecItemV2 recItemV2 = (MeasureRecItemV2) rawRecItem.getRecEntity();
        return recItemV2.getMeasure();
    }

    public static NDataModel.NamedColumn getDimension(RawRecItem rawRecItem) {
        return getDimensionRecItemV2(rawRecItem).getColumn();
    }

    public static String getDimensionDataType(RawRecItem rawRecItem) {
        return getDimensionRecItemV2(rawRecItem).getDataType();
    }

    private static DimensionRecItemV2 getDimensionRecItemV2(RawRecItem rawRecItem) {
        Preconditions.checkNotNull(rawRecItem);
        Preconditions.checkState(RawRecItem.RawRecType.DIMENSION == rawRecItem.getType());
        return (DimensionRecItemV2) rawRecItem.getRecEntity();
    }

    private static LayoutRecItemV2 getLayoutRecItemV2(RawRecItem rawRecItem) {
        Preconditions.checkNotNull(rawRecItem);
        Preconditions.checkState(RawRecItem.RawRecType.LAYOUT == rawRecItem.getType());
        return (LayoutRecItemV2) rawRecItem.getRecEntity();
    }

    public static LayoutEntity getLayout(RawRecItem rawRecItem) {
        return getLayoutRecItemV2(rawRecItem).getLayout();
    }

    public static boolean isAgg(RawRecItem rawRecItem) {
        return getLayoutRecItemV2(rawRecItem).isAgg();
    }

}
