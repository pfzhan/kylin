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
package org.apache.kylin.query;

import org.apache.kylin.query.engine.SchemaMapExtensionImpl;

import org.apache.kylin.query.engine.TableColumnAuthExtensionImpl;
import org.apache.kylin.query.engine.SchemaMapExtension;
import org.apache.kylin.query.engine.TableColumnAuthExtension;


public class QueryExtensionFactoryEnterprise extends QueryExtension.Factory {

    public QueryExtensionFactoryEnterprise() {
        super();
    }

    @Override
    protected SchemaMapExtension createSchemaMapExtension() {
        return new SchemaMapExtensionImpl();
    }

    @Override
    protected TableColumnAuthExtension createTableColumnAuthExtension() {
        return new TableColumnAuthExtensionImpl();
    }

}
