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
package io.kyligence.kap.metadata.cube.cuboid;

import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.TableExtDesc;
import org.apache.kylin.metadata.model.TblColRef;

import io.kyligence.kap.guava20.shaded.common.collect.ImmutableMultimap;
import io.kyligence.kap.guava20.shaded.common.collect.Lists;
import io.kyligence.kap.guava20.shaded.common.collect.Maps;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import lombok.Getter;
import lombok.val;

@Getter
public class ChooserContext {

    final NDataModel model;

    final ImmutableMultimap<Integer, Integer> fk2Pk;
    final Map<TblColRef, Integer> tblColMap = Maps.newHashMap();
    final Map<String, List<Integer>> primaryKeyColumnIds = Maps.newHashMap();
    final Map<String, List<Integer>> foreignKeyColumnIds = Maps.newHashMap();
    final Map<Integer, TableExtDesc.ColumnStats> columnStatMap = Maps.newHashMap();

    public ChooserContext(NDataModel model) {
        this.model = model;

        ImmutableMultimap.Builder<Integer, Integer> fk2PkBuilder = ImmutableMultimap.builder();

        initModelContext(model, fk2PkBuilder);
        if (isBatchFusionModel()) {
            NDataModel streamingModel = NDataModelManager
                    .getInstance(KylinConfig.getInstanceFromEnv(), model.getProject())
                    .getDataModelDesc(model.getFusionId());
            initModelContext(streamingModel, fk2PkBuilder);
        }

        this.fk2Pk = fk2PkBuilder.build();

    }

    public TableExtDesc.ColumnStats getColumnStats(TblColRef ref) {
        int colId = tblColMap.getOrDefault(ref, -1);
        return columnStatMap.get(colId);
    }

    public TblColRef convertToRef(Integer colId) {
        return model.getEffectiveCols().get(colId);
    }

    public Collection<TblColRef> convertToRefs(Collection<Integer> colIds) {
        List<TblColRef> refs = Lists.newArrayList();
        for (Integer colId : colIds) {
            refs.add(convertToRef(colId));
        }
        return refs;
    }

    public boolean isBatchFusionModel() {
        return model.isFusionModel() && !model.isStreaming();
    }

    private void initModelContext(NDataModel dataModel, ImmutableMultimap.Builder<Integer, Integer> fk2PkBuilder) {
        val effectiveCols = dataModel.getEffectiveCols();
        effectiveCols.forEach((key, value) -> tblColMap.put(value, key));

        val config = KylinConfig.getInstanceFromEnv();
        NTableMetadataManager tblMetaMgr = NTableMetadataManager.getInstance(config, model.getProject());
        effectiveCols.keySet().forEach(colId -> {
            TableExtDesc.ColumnStats stats = TableExtDesc.ColumnStats.getColumnStats(tblMetaMgr,
                    effectiveCols.get(colId));
            columnStatMap.put(colId, stats);
        });

        if (CollectionUtils.isEmpty(dataModel.getJoinTables())) {
            return;
        }
        dataModel.getJoinTables().forEach(joinDesc -> {
            List<Integer> pks = Lists.newArrayList();
            List<Integer> fks = Lists.newArrayList();
            primaryKeyColumnIds.put(joinDesc.getAlias(), Arrays.stream(joinDesc.getJoin().getPrimaryKeyColumns())
                    .map(tblColMap::get).collect(Collectors.toList()));
            foreignKeyColumnIds.put(joinDesc.getAlias(), Arrays.stream(joinDesc.getJoin().getForeignKeyColumns())
                    .map(tblColMap::get).collect(Collectors.toList()));

            val join = joinDesc.getJoin();
            int n = join.getForeignKeyColumns().length;
            for (int i = 0; i < n; i++) {
                val pk = join.getPrimaryKeyColumns()[i];
                val fk = join.getForeignKeyColumns()[i];
                val pkId = tblColMap.get(pk);
                val fkId = tblColMap.get(fk);
                pks.add(pkId);
                fks.add(fkId);
                fk2PkBuilder.put(fkId, pkId);
            }
            primaryKeyColumnIds.put(joinDesc.getAlias(), pks);
            foreignKeyColumnIds.put(joinDesc.getAlias(), fks);
        });
    }

}
