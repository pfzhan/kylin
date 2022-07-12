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

package io.kyligence.kap.rest.service;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;

import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.rest.request.MergeAndUpdateTableExtRequest;

import java.util.List;

public class TableMetadataBaseService {
    public void mergeAndUpdateTableExt(String project, MergeAndUpdateTableExtRequest request) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project)
                    .mergeAndUpdateTableExt(request.getOrigin(), request.getOther());
            return null;
        }, project);
    }

    public void saveTableExt(String project, TableExtDesc tableExt) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project).saveTableExt(tableExt);
            return null;
        }, project);
    }

    public void updateTableDesc(String project, TableDesc tableDesc) {
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project).updateTableDesc(tableDesc);
            return null;
        }, project);
    }

    public TableDesc getTableDesc(String project, String tableName) {
        return NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project).getTableDesc(tableName);
    }
}
