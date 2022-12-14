/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kylin.rest.delegate;

import java.util.List;

import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.rest.request.MergeAndUpdateTableExtRequest;
import org.apache.kylin.rest.service.TableExtService;
import org.springframework.stereotype.Component;

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

    public List<String> getTableNamesByFuzzyKey(String project, String fuzzyKey) {
        return getDelegate().getTableNamesByFuzzyKey(project, fuzzyKey);
    }
}
