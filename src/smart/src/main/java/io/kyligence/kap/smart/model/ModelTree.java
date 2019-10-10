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

package io.kyligence.kap.smart.model;

import java.util.Collection;
import java.util.Map;

import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TableRef;
import org.apache.kylin.query.relnode.OLAPContext;

import com.google.common.collect.Maps;

import lombok.Getter;

@Getter
public class ModelTree {
    private final Collection<OLAPContext> olapContexts;
    private final Map<String, JoinTableDesc> joins;
    private final Map<TableRef, String> tableRefAliasMap;
    private final TableDesc rootFactTable;

    public ModelTree(TableDesc rootFactTable, Collection<OLAPContext> contexts, Map<String, JoinTableDesc> joins,
            Map<TableRef, String> tableRefAliasMap) {
        this.rootFactTable = rootFactTable;
        this.olapContexts = contexts;
        this.joins = joins;
        this.tableRefAliasMap = tableRefAliasMap;
    }

    public ModelTree(TableDesc rootFactTable, Collection<OLAPContext> contexts, Map<String, JoinTableDesc> joins) {
        this.rootFactTable = rootFactTable;
        this.olapContexts = contexts;
        this.joins = joins;
        this.tableRefAliasMap = Maps.newHashMap();
    }
}
