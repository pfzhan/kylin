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

package io.kyligence.kap.smart.query;

import java.util.Collection;

import org.apache.kylin.query.relnode.OLAPContext;
import org.apache.kylin.storage.StorageContext;

import io.kyligence.kap.cube.model.NDataflow;
import io.kyligence.kap.storage.NDataStorageQueryRequest;

public class QueryRecord {
    private NDataflow dataflow;
    private NDataStorageQueryRequest gtRequest;
    private SQLResult sqlResult;
    private StorageContext storageContext;
    private Collection<OLAPContext> olapContexts;

    public StorageContext getStorageContext() {
        return storageContext;
    }

    public void setStorageContext(StorageContext storageContext) {
        this.storageContext = storageContext;
    }

    public NDataflow getCubeInstance() {
        return dataflow;
    }

    public void setCubeInstance(NDataflow cubeInstance) {
        this.dataflow = cubeInstance;
    }

    public NDataStorageQueryRequest getGtRequest() {
        return gtRequest;
    }

    public void setGtRequest(NDataStorageQueryRequest gtRequest) {
        this.gtRequest = gtRequest;
    }

    public SQLResult getSqlResult() {
        return sqlResult;
    }

    public void setSqlResult(SQLResult sqlResult) {
        this.sqlResult = sqlResult;
    }

    public Collection<OLAPContext> getOLAPContexts() {
        return olapContexts;
    }

    public void setOLAPContexts(Collection<OLAPContext> olapContexts) {
        this.olapContexts = olapContexts;
    }
}
