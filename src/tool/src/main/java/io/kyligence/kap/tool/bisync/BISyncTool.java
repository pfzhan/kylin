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

package io.kyligence.kap.tool.bisync;

import io.kyligence.kap.tool.bisync.model.SyncModel;
import io.kyligence.kap.tool.bisync.tableau.TableauDataSourceConverter;

import java.util.Set;

public class BISyncTool {

    public static BISyncModel dumpToBISyncModel(SyncContext syncContext) {
        SyncModel syncModel = new SyncModelBuilder(syncContext).buildSourceSyncModel();
        return getBISyncModel(syncContext, syncModel);
    }

    private static BISyncModel getBISyncModel(SyncContext syncContext, SyncModel syncModel) {
        switch (syncContext.getTargetBI()) {
            case TABLEAU_ODBC_TDS:
            case TABLEAU_CONNECTOR_TDS:
                return new TableauDataSourceConverter().convert(syncModel, syncContext);
            default:
                throw new IllegalArgumentException();
        }
    }

    public static BISyncModel dumpHasPermissionToBISyncModel(SyncContext syncContext,
                                                             Set<String> authTables, Set<String> authColumns) {
        SyncModel syncModel = new SyncModelBuilder(syncContext)
                .buildHasPermissionSourceSyncModel(authTables, authColumns);
        return getBISyncModel(syncContext, syncModel);
    }
}
