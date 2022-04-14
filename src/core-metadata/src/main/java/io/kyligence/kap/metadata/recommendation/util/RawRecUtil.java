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

package io.kyligence.kap.metadata.recommendation.util;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nonnull;

import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ParameterDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;

import io.kyligence.kap.metadata.cube.model.LayoutEntity;
import io.kyligence.kap.metadata.model.ComputedColumnDesc;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.recommendation.candidate.RawRecItem;
import io.kyligence.kap.metadata.recommendation.entity.CCRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.LayoutRecItemV2;
import io.kyligence.kap.metadata.recommendation.entity.MeasureRecItemV2;

public class RawRecUtil {

    public static final String TABLE_COLUMN_SEPARATOR = "\\$";

    private RawRecUtil() {
    }

    public static ColumnDesc findColumn(String idOrColumnName, @Nonnull TableDesc tableDesc) {
        ColumnDesc[] columns = tableDesc.getColumns();
        ColumnDesc dependColumn = tableDesc.findColumnByName(idOrColumnName);
        if (dependColumn == null) {
            // compatible to old version
            dependColumn = columns[Integer.parseInt(idOrColumnName)];
        }
        return dependColumn;
    }

    public static String dimensionUniqueContent(TblColRef tblColRef, Map<String, ComputedColumnDesc> ccMap) {
        return colUniqueName(tblColRef, ccMap);
    }

    public static String measureUniqueContent(NDataModel.Measure measure, Map<String, ComputedColumnDesc> ccMap) {
        Set<String> paramNames = Sets.newHashSet();
        List<ParameterDesc> parameters = measure.getFunction().getParameters();
        parameters.forEach(param -> {
            TblColRef colRef = param.getColRef();
            if (colRef == null) {
                paramNames.add(String.valueOf(Integer.MAX_VALUE));
                return;
            }
            paramNames.add(colUniqueName(colRef, ccMap));
        });
        return String.format(Locale.ROOT, "%s__%s", measure.getFunction().getExpression(),
                String.join("__", paramNames));
    }

    private static String colUniqueName(TblColRef tblColRef, Map<String, ComputedColumnDesc> ccMap) {
        final ColumnDesc columnDesc = tblColRef.getColumnDesc();
        String uniqueName;
        if (columnDesc.isComputedColumn()) {
            /* if cc is new, unique_name forward to its uuid,
             * otherwise table_alias.column_id
             */
            ComputedColumnDesc cc = ccMap.get(columnDesc.getIdentity());
            if (cc.getUuid() != null) {
                uniqueName = cc.getUuid();
            } else {
                uniqueName = tblColRef.getTableRef().getAlias() + "$" + columnDesc.getName();
            }
        } else {
            uniqueName = tblColRef.getTableRef().getAlias() + "$" + columnDesc.getName();
        }
        return uniqueName;
    }

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

    private static LayoutRecItemV2 getLayoutRecItemV2(RawRecItem rawRecItem) {
        Preconditions.checkArgument(rawRecItem != null && rawRecItem.isLayoutRec());
        return (LayoutRecItemV2) rawRecItem.getRecEntity();
    }

    public static LayoutEntity getLayout(RawRecItem rawRecItem) {
        return getLayoutRecItemV2(rawRecItem).getLayout();
    }
}
