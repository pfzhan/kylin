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

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.util.SpringContext;
import org.springframework.stereotype.Component;

import io.kyligence.kap.rest.request.MergeAndUpdateTableExtRequest;
import io.kyligence.kap.rest.service.TableExtService;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TableMetadataInvoker extends TableMetadataBaseInvoker {
    private static TableMetadataContract delegate = null;

    public static void setDelegate(TableMetadataContract delegate) {
        if (TableMetadataInvoker.delegate != null) {
            log.warn("Delegate is replaced as {}, origin value is {}", delegate, TableMetadataInvoker.delegate);
        }
        TableMetadataInvoker.delegate = delegate;
    }

    private TableMetadataContract getDelegate() {
        if (delegate == null) {
            // Generally delegate will be set in ContractConfig, here is used for test
            if (SpringContext.getApplicationContext() == null) {
                return new TableExtService();
            }
            return SpringContext.getBean(TableExtService.class);
        }
        return delegate;
    }

    @Override
    public void mergeAndUpdateTableExt(String project, MergeAndUpdateTableExtRequest request) {
        getDelegate().mergeAndUpdateTableExt(project, request);
    }

    @Override
    public void saveTableExt(String project, TableExtDesc tableExt) {
        getDelegate().saveTableExt(project, tableExt);
    }

    @Override
    public void updateTableDesc(String project, TableDesc tableDesc) {
        getDelegate().updateTableDesc(project, tableDesc);
    }

    public TableDesc getTableDesc(String project, String tableName) {
        return getDelegate().getTableDesc(project, tableName);
    }
}
