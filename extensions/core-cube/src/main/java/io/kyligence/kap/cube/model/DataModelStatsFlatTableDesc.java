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

package io.kyligence.kap.cube.model;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.IJoinedFlatTableDesc;
import org.apache.kylin.metadata.model.ISegment;
import org.apache.kylin.metadata.model.ModelDimensionDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
Used to create a flat table only contains dimensions for data model stats.
 */

public class DataModelStatsFlatTableDesc implements IJoinedFlatTableDesc {

    private static final Logger logger = LoggerFactory.getLogger(DataModelStatsFlatTableDesc.class);

    private DataModelDesc dataModelDesc;
    private List<TblColRef> columnList = new ArrayList<>();

    public DataModelStatsFlatTableDesc(DataModelDesc dataModelDesc) {
        this.dataModelDesc = dataModelDesc;
        init();
    }

    private void init() {
        for (ModelDimensionDesc mdDesc : dataModelDesc.getDimensions()) {
            for (String col : mdDesc.getColumns()) {
                TblColRef tblColRef = dataModelDesc.findColumn(mdDesc.getTable(), col);
                if (tblColRef == null) {
                    logger.error("Dimension: table name: {}; col name: {}", mdDesc.getTable(), col);
                }
                columnList.add(tblColRef);
            }
        }
    }

    @Override
    public String getTableName() {
        return "kylin_intermediate_" + dataModelDesc.getName() + "_stats";
    }

    @Override
    public DataModelDesc getDataModel() {
        return this.dataModelDesc;
    }

    @Override
    public List<TblColRef> getAllColumns() {
        return columnList;
    }

    @Override
    public int getColumnIndex(TblColRef colRef) {
        return 0;
    }

    @Override
    public long getSourceOffsetStart() {
        return 0;
    }

    @Override
    public long getSourceOffsetEnd() {
        return 0;
    }

    @Override
    public TblColRef getDistributedBy() {
        return null;
    }

    @Override
    public ISegment getSegment() {
        return null;
    }
}
