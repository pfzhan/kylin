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

package org.apache.kylin.rest.response;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.kylin.metadata.querymeta.TableMetaWithType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.Getter;

public class TableMetaCacheResultV2 implements Serializable {
    protected static final long serialVersionUID = 1L;

    private static final Logger logger = LoggerFactory.getLogger(TableMetaCacheResultV2.class);

    @Getter
    private List<TableMetaWithType> tableMetaList = new ArrayList<>();

    @Getter
    private String signature;

    public TableMetaCacheResultV2() {
    }

    public TableMetaCacheResultV2(List<TableMetaWithType> tableMetaList, String signature) {
        this.tableMetaList = tableMetaList;
        this.signature = signature;
    }

    public List<String> getTables() {
        return tableMetaList.stream().map(meta -> meta.getTABLE_SCHEM() + "." + meta.getTABLE_NAME())
                .collect(Collectors.toList());
    }
}
