/**
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

package io.kyligence.kap.query.security;

import java.util.Collection;
import java.util.Hashtable;
import java.util.Map;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.cube.CubeInstance;
import org.apache.kylin.cube.model.CubeDesc;
import org.apache.kylin.metadata.filter.TupleFilter;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.realization.IRealization;
import org.apache.kylin.query.relnode.OLAPAuthentication;
import org.apache.kylin.query.relnode.OLAPContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KapAccessDecisionMaker implements OLAPContext.IAccessController {
    private final static Logger logger = LoggerFactory.getLogger(KapAccessDecisionMaker.class);
    private static final IACLMetaData metaSource = new KapAclReader();

    public TupleFilter rowFilter(IRealization realization, OLAPAuthentication olapAuthentication) {

        if (null == realization || !(realization instanceof CubeInstance))
            return null;
        CubeDesc cubeDesc = ((CubeInstance) realization).getDescriptor();
        IRowFilter iRowFilter = new KapRowFilter();
        return iRowFilter.getRowFilter(olapAuthentication, cubeDesc.listAllColumns(), cubeDesc.getOverrideKylinProps());
    }

    public void columnFilter(Collection<TblColRef> columns, OLAPAuthentication olapAuthentication) {
        Hashtable<String, String> aclColumns = IACLMetaData.accessControlColumnsByUser.get(olapAuthentication.getUsername().toLowerCase());
        for (Map.Entry<String, String> attr : aclColumns.entrySet()) {
            for (TblColRef tblColRef : columns) {
                if (tblColRef.getCanonicalName().toLowerCase().contains(attr.getKey()) && attr.getValue().equalsIgnoreCase(IACLMetaData.DENY)) {
                    throw new IllegalArgumentException("Current User: " + olapAuthentication.getUsername() + " is not allowed to access column: " + tblColRef.getCanonicalName());
                }
            }
        }
    }

    @Override
    public TupleFilter check(OLAPAuthentication olapAuthentication, Collection<TblColRef> columns, IRealization realization) throws IllegalArgumentException {

        boolean isACLEnable = KapConfig.getInstanceFromEnv().getCellLevelSecurityEnable().equalsIgnoreCase("true");

        if (isACLEnable) {
            logger.info("User access control is enable!!!!");
        } else {
            return null;
        }

        if (null == olapAuthentication.getUsername())
            return null;
        if (IACLMetaData.accessControlColumnsByUser.isEmpty())
            return null;

        Collection<TblColRef> requiredColumns;

        if (0 == columns.size()) {
            requiredColumns = realization.getAllColumns();
        } else {
            requiredColumns = columns;
        }
        columnFilter(requiredColumns, olapAuthentication);
        TupleFilter tupleFilter = rowFilter(realization, olapAuthentication);
        return tupleFilter;
    }
}
