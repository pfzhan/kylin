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
package io.kyligence.kap.query.util;

import java.util.LinkedHashMap;

import org.apache.kylin.query.relnode.ColumnRowType;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.alias.AliasMapping;

public class QueryAliasMatchInfo extends AliasMapping {
    // each alias's ColumnRowType
    private LinkedHashMap<String, ColumnRowType> alias2CRT;

    // for model view
    private NDataModel model;

    public QueryAliasMatchInfo(BiMap<String, String> aliasMapping, LinkedHashMap<String, ColumnRowType> alias2CRT) {
        super(aliasMapping);
        this.alias2CRT = alias2CRT;
    }

    private QueryAliasMatchInfo(BiMap<String, String> aliasMapping, NDataModel model) {
        super(aliasMapping);
        this.model = model;
    }

    public static QueryAliasMatchInfo fromModelView(String queryTableAlias, NDataModel model) {
        BiMap<String, String> map = HashBiMap.create();
        map.put(queryTableAlias, model.getAlias());
        return new QueryAliasMatchInfo(map, model);
    }

    LinkedHashMap<String, ColumnRowType> getAlias2CRT() {
        return alias2CRT;
    }

    public boolean isModelView() {
        return model != null;
    }

    public NDataModel getModel() {
        return model;
    }

}
