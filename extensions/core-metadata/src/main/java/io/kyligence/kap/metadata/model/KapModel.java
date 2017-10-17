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

package io.kyligence.kap.metadata.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.commons.lang.mutable.MutableInt;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.util.Pair;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.ComputedColumnDesc;
import org.apache.kylin.metadata.model.DataModelDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

@SuppressWarnings("serial")
@JsonAutoDetect(fieldVisibility = Visibility.NONE, getterVisibility = Visibility.NONE, isGetterVisibility = Visibility.NONE, setterVisibility = Visibility.NONE)
public class KapModel extends DataModelDesc {

    @SuppressWarnings("unused")
    private static final Logger logger = LoggerFactory.getLogger(KapModel.class);
    
    @JsonProperty("computed_columns")
    @JsonInclude(JsonInclude.Include.NON_DEFAULT)
    private List<ComputedColumnDesc> computedColumnDescs = Lists.newArrayList();

    // don't use unless you're sure, for jackson only
    public KapModel() {
        super();
    }

    @Override
    public void init(KylinConfig config, Map<String, TableDesc> originalTables, List<DataModelDesc> otherModels) {
        // tweak the tables according to Computed Columns defined in model
        Map<String, TableDesc> tables = Maps.newHashMap();
        for (Map.Entry<String, TableDesc> entry : originalTables.entrySet()) {
            String s = entry.getKey();
            TableDesc tableDesc = entry.getValue();
            TableDesc extendedTableDesc = tableDesc.appendColumns(createComputedColumns(tableDesc));
            tables.put(s, extendedTableDesc);
        }

        super.init(config, tables, otherModels);
        
        initComputedColumns(otherModels);
    }

    private void initComputedColumns(List<DataModelDesc> otherModels) {
        Preconditions.checkNotNull(otherModels);

        List<Pair<ComputedColumnDesc, KapModel>> existingCCs = Lists.newArrayList();

        for (DataModelDesc dataModelDesc : otherModels) {
            if (dataModelDesc instanceof KapModel) {
                KapModel otherModel = (KapModel) dataModelDesc;
                if (!StringUtils.equals(otherModel.getName(), this.getName())) {
                    for (ComputedColumnDesc cc : otherModel.getComputedColumnDescs()) {
                        existingCCs.add(Pair.newPair(cc, otherModel));
                    }
                }
            }
        }

        for (ComputedColumnDesc newCC : this.computedColumnDescs) {

            newCC.init(getAliasMap(), getRootFactTable().getAlias());
            final String newCCFullName = newCC.getFullName();
            final String newCCColumnName = newCC.getColumnName();

            for (Pair<ComputedColumnDesc, KapModel> pair : existingCCs) {
                DataModelDesc dataModelDesc = pair.getSecond();
                ComputedColumnDesc cc = pair.getFirst();

                if (StringUtils.equalsIgnoreCase(cc.getFullName(), newCCFullName) && !(cc.equals(newCC))) {
                    throw new IllegalArgumentException(String.format(
                            "Column name for computed column %s is already used in model %s, you should apply the same expression ' %s ' here, or use a different column name.",
                            newCCFullName, dataModelDesc.getName(), cc.getExpression()));
                }

                if (isTwoCCDefinitionEquals(cc.getExpression(), newCC.getExpression())
                        && !StringUtils.equalsIgnoreCase(cc.getColumnName(), newCCColumnName)) {
                    throw new IllegalArgumentException(String.format(
                            "Expression %s in computed column %s is already defined by computed column %s from model %s, you should use the same column name: ' %s ' .",
                            newCC.getExpression(), newCCFullName, cc.getFullName(), dataModelDesc.getName(),
                            cc.getColumnName()));
                }
            }
            existingCCs.add(Pair.newPair(newCC, this));
        }
    }

    private boolean isTwoCCDefinitionEquals(String definition0, String definition1) {
        definition0 = definition0.replaceAll("\\s*", "");
        definition1 = definition1.replaceAll("\\s*", "");
        return definition0.equalsIgnoreCase(definition1);
    }

    private ColumnDesc[] createComputedColumns(final TableDesc tableDesc) {
        final MutableInt id = new MutableInt(tableDesc.getColumnCount());
        return FluentIterable.from(this.computedColumnDescs).filter(new Predicate<ComputedColumnDesc>() {
            @Override
            public boolean apply(@Nullable ComputedColumnDesc input) {
                return tableDesc.getIdentity().equalsIgnoreCase(input.getTableIdentity());
            }
        }).transform(new Function<ComputedColumnDesc, ColumnDesc>() {
            @Nullable
            @Override
            public ColumnDesc apply(@Nullable ComputedColumnDesc input) {
                id.increment();
                ColumnDesc columnDesc = new ColumnDesc(id.toString(), input.getColumnName(), input.getDatatype(),
                        input.getComment(), null, null, input.getExpression());
                return columnDesc;
            }
        }).toArray(ColumnDesc.class);
    }

    public ComputedColumnDesc findCCByCCColumnName(final String columnName) {
        return Iterables.find(this.computedColumnDescs, new Predicate<ComputedColumnDesc>() {
            @Override
            public boolean apply(@Nullable ComputedColumnDesc input) {
                Preconditions.checkNotNull(input);
                return columnName.equals(input.getColumnName());
            }
        });
    }

    public List<ComputedColumnDesc> getComputedColumnDescs() {
        return computedColumnDescs;
    }

    public void setComputedColumnDescs(List<ComputedColumnDesc> computedColumnDescs) {
        this.computedColumnDescs = computedColumnDescs;
    }

    public Set<String> getComputedColumnNames() {
        Set<String> ccColumnNames = Sets.newHashSet();
        for (ComputedColumnDesc cc : this.getComputedColumnDescs()) {
            ccColumnNames.add(cc.getColumnName());
        }
        return Collections.unmodifiableSet(ccColumnNames);
    }

    public static KapModel getCopyOf(KapModel orig) {
        KapModel copy = (KapModel) DataModelDesc.copy(orig, new KapModel());
        copy.computedColumnDescs = orig.computedColumnDescs;
        return copy;
    }

}
