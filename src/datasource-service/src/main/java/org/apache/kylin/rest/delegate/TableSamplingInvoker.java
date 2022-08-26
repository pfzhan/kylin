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
import java.util.Set;

import org.apache.kylin.rest.util.SpringContext;
import org.apache.kylin.rest.service.TableSamplingSupporter;
import org.springframework.stereotype.Component;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@Component
public class TableSamplingInvoker {

    private static TableSamplingContract delegate = null;

    public static void setDelegate(TableSamplingContract delegate) {
        if (TableSamplingInvoker.delegate != null) {
            log.warn("Delegate is replaced as {}, origin value is {}", delegate, TableSamplingInvoker.delegate);
        }
        TableSamplingInvoker.delegate = delegate;
    }

    public TableSamplingContract getDelegate() {
        if (delegate == null) {
            return SpringContext.getBean(TableSamplingSupporter.class);
        }
        return delegate;
    }

    public List<String> sampling(Set<String> tables, String project, int rows, int priority, String yarnQueue,
                          Object tag) {
        return getDelegate().sampling(tables, project, rows, priority, yarnQueue, tag);
    }

}
