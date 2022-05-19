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
package io.kyligence.kap.rest.delegate;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinRuntimeException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.util.SpringContext;

import io.kyligence.kap.common.persistence.metadata.JdbcMetadataStore;
import io.kyligence.kap.common.persistence.metadata.MetadataStore;
import io.kyligence.kap.rest.request.MergeAndUpdateTableExtRequest;
import io.kyligence.kap.rest.service.TableMetadataBaseServer;

public class TableMetadataBaseInvoker {
    public static TableMetadataBaseInvoker getInstance() {
        MetadataStore metadataStore = ResourceStore.getKylinMetaStore(KylinConfig.getInstanceFromEnv())
                .getMetadataStore();
        if (!(metadataStore instanceof JdbcMetadataStore) && KylinConfig.getInstanceFromEnv().isDataLoadingNode()) {
            throw new KylinRuntimeException("This request cannot be route to metadata server");
        }
        if (SpringContext.getApplicationContext() == null) {
            // for UT
            return new TableMetadataBaseInvoker();
        } else {
            return SpringContext.getBean(TableMetadataBaseInvoker.class);
        }
    }

    private final TableMetadataBaseServer tableMetadataBaseServer = new TableMetadataBaseServer();

    public void mergeAndUpdateTableExt(String project, MergeAndUpdateTableExtRequest request) {
        tableMetadataBaseServer.mergeAndUpdateTableExt(project, request);
    }

    public void saveTableExt(String project, TableExtDesc tableExt) {
        tableMetadataBaseServer.saveTableExt(project, tableExt);
    }

    public void updateTableDesc(String project, TableDesc tableDesc) {
        tableMetadataBaseServer.updateTableDesc(project, tableDesc);
    }

}
