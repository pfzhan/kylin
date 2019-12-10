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

package io.kyligence.kap.metadata.acl;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.model.util.ComputedColumnUtil;

public class AclTCRManager {

    private static final Logger logger = LoggerFactory.getLogger(AclTCRManager.class);

    private static final String IDENTIFIER_FORMAT = "%s.%s";

    public static AclTCRManager getInstance(KylinConfig config, String project) {
        return config.getManager(project, AclTCRManager.class);
    }

    static AclTCRManager newInstance(KylinConfig config, String project) {
        return new AclTCRManager(config, project);
    }

    private final KylinConfig config;
    private final String project;

    private CachedCrudAssist<AclTCR> userCrud;

    private CachedCrudAssist<AclTCR> groupCrud;

    public AclTCRManager(KylinConfig config, String project) {
        if (!UnitOfWork.isAlreadyInTransaction())
            logger.info("Initializing AclGroupManager with KylinConfig Id: {}", System.identityHashCode(config));

        this.config = config;
        this.project = project;

        ResourceStore metaStore = ResourceStore.getKylinMetaStore(this.config);
        userCrud = new CachedCrudAssist<AclTCR>(metaStore, String.format("/%s/acl/user", project), AclTCR.class) {
            @Override
            protected AclTCR initEntityAfterReload(AclTCR acl, String resourceName) {
                acl.init(resourceName);
                return acl;
            }
        };
        userCrud.reloadAll();

        groupCrud = new CachedCrudAssist<AclTCR>(metaStore, String.format("/%s/acl/group", project), AclTCR.class) {
            @Override
            protected AclTCR initEntityAfterReload(AclTCR acl, String resourceName) {
                acl.init(resourceName);
                return acl;
            }
        };
        groupCrud.reloadAll();
    }

    public void unloadTable(String dbTblName) {
        userCrud.listAll().forEach(aclTCR -> {
            if (Objects.isNull(aclTCR.getTable())) {
                return;
            }
            AclTCR copied = userCrud.copyForWrite(aclTCR);
            copied.getTable().remove(dbTblName);
            userCrud.save(copied);
        });

        groupCrud.listAll().forEach(aclTCR -> {
            if (Objects.isNull(aclTCR.getTable())) {
                return;
            }
            AclTCR copied = groupCrud.copyForWrite(aclTCR);
            copied.getTable().remove(dbTblName);
            groupCrud.save(copied);
        });
    }

    public AclTCR getAclTCR(String sid, boolean principal) {
        if (principal) {
            return userCrud.get(sid);
        }
        return groupCrud.get(sid);
    }

    public void updateAclTCR(AclTCR updateTo, String sid, boolean principal) {
        updateTo.init(sid);
        if (principal) {
            doUpdate(updateTo, sid, userCrud);
        } else {
            doUpdate(updateTo, sid, groupCrud);
        }
    }

    private void doUpdate(AclTCR updateTo, String sid, CachedCrudAssist<AclTCR> crud) {
        AclTCR copied;
        AclTCR origin = crud.get(sid);
        if (Objects.isNull(origin)) {
            copied = updateTo;
        } else {
            copied = crud.copyForWrite(origin);
            copied.setTable(updateTo.getTable());
        }
        crud.save(copied);
    }

    public void revokeAclTCR(String sid, boolean principal) {
        if (principal) {
            userCrud.delete(sid);
        } else {
            groupCrud.delete(sid);
        }
    }

    public List<AclTCR> getAclTCRs(String username, Set<String> groups) {
        final List<AclTCR> result = Lists.newArrayList();
        if (StringUtils.isNotEmpty(username)) {
            result.add(userCrud.get(username));
        }
        if (CollectionUtils.isNotEmpty(groups)) {
            groups.forEach(g -> result.add(groupCrud.get(g)));
        }
        return result.stream().filter(Objects::nonNull).collect(Collectors.toList());
    }

    // [ "DB1.TABLE1" ]
    public Set<String> getAuthorizedTables(String username, Set<String> groups) {
        final List<AclTCR> all = getAclTCRs(username, groups);
        if (isTablesAuthorized(all)) {
            return getAllTables();
        }
        return all.stream().map(aclTCR -> aclTCR.getTable().keySet()).flatMap(Set::stream).collect(Collectors.toSet());
    }

    // [ DB1.TABLE1.COLUMN1 ]
    public Set<String> getAuthorizedColumns(String username, Set<String> groups) {
        final List<AclTCR> all = getAclTCRs(username, groups);
        if (isTablesAuthorized(all)) {
            return getAllColumns();
        }
        return all.stream().map(aclTCR -> aclTCR.getTable().entrySet().stream().map(entry -> {
            TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(entry.getKey());
            if (Objects.isNull(tableDesc)) {
                return Sets.<String> newHashSet();
            }
            if (Objects.isNull(entry.getValue()) || Objects.isNull(entry.getValue().getColumn())) {
                return Optional.ofNullable(tableDesc.getColumns()).map(Arrays::stream).orElseGet(Stream::empty)
                        .map(columnDesc -> getDbTblCols(tableDesc, columnDesc)).flatMap(Set::stream)
                        .collect(Collectors.toSet());
            }
            return Optional.ofNullable(tableDesc.getColumns()).map(Arrays::stream).orElseGet(Stream::empty)
                    .filter(columnDesc -> entry.getValue().getColumn().contains(columnDesc.getName()))
                    .map(columnDesc -> getDbTblCols(tableDesc, columnDesc)).flatMap(Set::stream)
                    .collect(Collectors.toSet());
        }).flatMap(Set::stream).collect(Collectors.toSet())).flatMap(Set::stream).collect(Collectors.toSet());
    }

    public Optional<String> failFastUnauthorizedTableColumn(String username, Set<String> groups,
            Map<String, Set<String>> tableColumns) {
        if (MapUtils.isEmpty(tableColumns)) {
            return Optional.empty();
        }
        final List<AclTCR> all = getAclTCRs(username, groups);
        if (isTablesAuthorized(all)) {
            return Optional.empty();
        }

        final Map<String, Set<String>> authorizedTableColumns = getAuthorizedTableColumns(all);

        //fail-fast
        return tableColumns.entrySet().stream().map(entry -> {
            // fail-fast table
            if (!authorizedTableColumns.containsKey(entry.getKey())) {
                return Optional.of(entry.getKey());
            }
            if (CollectionUtils.isEmpty(entry.getValue())
                    || Objects.isNull(authorizedTableColumns.get(entry.getKey()))) {
                return Optional.<String> empty();
            }
            // fail-fast column
            return entry.getValue().stream()
                    .filter(colName -> !authorizedTableColumns.get(entry.getKey()).contains(colName))
                    .map(colName -> String.format(IDENTIFIER_FORMAT, entry.getKey(), colName)).findAny();
        }).filter(Optional::isPresent).findAny().orElse(Optional.empty());
    }

    public Map<String, String> getTableColumnConcatWhereCondition(String username, Set<String> groups) {
        // <DB1.TABLE1, COLUMN_CONCAT_WHERE_CONDITION>
        Map<String, String> result = Maps.newHashMap();
        final List<AclTCR> all = getAclTCRs(username, groups);
        if (isTablesAuthorized(all)) {
            return result;
        }

        final Map<String, List<PrincipalRowSet>> dbTblPrincipals = getTblPrincipalSet(all);

        if (MapUtils.isEmpty(dbTblPrincipals)) {
            return result;
        }

        dbTblPrincipals.forEach((dbTblName, principals) -> {
            TableDesc tableDesc = NTableMetadataManager.getInstance(config, project).getTableDesc(dbTblName);
            if (Objects.isNull(tableDesc)) {
                return;
            }
            final Map<String, String> columnType = Optional.ofNullable(tableDesc.getColumns()).map(Arrays::stream)
                    .orElseGet(Stream::empty)
                    .map(columnDesc -> new AbstractMap.SimpleEntry<>(columnDesc.getName(), columnDesc.getTypeName()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));

            result.put(dbTblName, concatPrincipalConditions(principals.stream().map(p -> {
                final ColumnToConds columnConditions = new ColumnToConds();
                p.getRowSets().forEach(r -> columnConditions.put(r.getColumnName(),
                        r.getValues().stream().map(ColumnToConds.Cond::new).collect(Collectors.toList())));
                return ColumnToConds.concatConds(columnConditions, columnType);
            }).collect(Collectors.toList())));
        });

        return result;
    }

    private String concatPrincipalConditions(List<String> conditions) {
        final String joint = String.join(" OR ", conditions);
        if (conditions.size() > 1) {
            StringBuilder sb = new StringBuilder("(");
            sb.append(joint);
            sb.append(")");
            return sb.toString();
        }
        return joint;
    }

    public Optional<Set<String>> getAuthorizedRows(String dbTblName, String colName, List<AclTCR> aclTCRS) {
        Set<String> rows = Sets.newHashSet();
        for (AclTCR aclTCR : aclTCRS) {
            if (Objects.isNull(aclTCR.getTable())) {
                return Optional.empty();
            }

            if (!aclTCR.getTable().containsKey(dbTblName)) {
                continue;
            }

            AclTCR.ColumnRow columnRow = aclTCR.getTable().get(dbTblName);
            if (Objects.isNull(columnRow) || Objects.isNull(columnRow.getRow())) {
                return Optional.empty();
            }

            AclTCR.Row row = columnRow.getRow();
            if (!row.containsKey(colName) || Objects.isNull(row.get(colName))) {
                return Optional.empty();
            }
            rows.addAll(row.get(colName));
        }
        return Optional.of(rows);
    }

    private boolean isRowAuthorized(String dbTblName, String colName, final AclTCR e) {
        if (!e.getTable().containsKey(dbTblName)) {
            return false;
        }
        if (Objects.isNull(e.getTable().get(dbTblName)) || Objects.isNull(e.getTable().get(dbTblName).getRow())) {
            return true;
        }
        if (!e.getTable().get(dbTblName).getRow().containsKey(colName)) {
            return false;
        }
        return Objects.isNull(e.getTable().get(dbTblName).getRow().get(colName));
    }

    private Map<String, List<PrincipalRowSet>> getTblPrincipalSet(final List<AclTCR> acls) {
        final Map<String, List<PrincipalRowSet>> dbTblPrincipals = Maps.newHashMap();
        acls.forEach(tcr -> tcr.getTable().forEach((dbTblName, columnRow) -> {
            if (Objects.isNull(columnRow) || Objects.isNull(columnRow.getRow())) {
                return;
            }
            final PrincipalRowSet principal = new PrincipalRowSet();
            columnRow.getRow().forEach((colName, realRow) -> {
                if (Objects.isNull(realRow) || acls.stream().filter(e -> !e.equals(tcr))
                        .anyMatch(e -> isRowAuthorized(dbTblName, colName, e))) {
                    return;
                }
                principal.getRowSets().add(new RowSet(colName, realRow));
            });
            if (CollectionUtils.isEmpty(principal.getRowSets())) {
                return;
            }
            if (!dbTblPrincipals.containsKey(dbTblName)) {
                dbTblPrincipals.put(dbTblName, Lists.newArrayList());
            }
            dbTblPrincipals.get(dbTblName).add(principal);
        }));
        return dbTblPrincipals;
    }

    private boolean isTablesAuthorized(List<AclTCR> all) {
        //default all tables were authorized
        return all.stream().anyMatch(aclTCR -> Objects.isNull(aclTCR.getTable()));
    }

    private Set<String> getAllTables() {
        return NTableMetadataManager.getInstance(config, project).listAllTables().stream().map(TableDesc::getIdentity)
                .collect(Collectors.toSet());
    }

    private Set<String> getAllColumns() {
        return NTableMetadataManager.getInstance(config, project).listAllTables().stream()
                .map(tableDesc -> Optional.ofNullable(tableDesc.getColumns()).map(Arrays::stream)
                        .orElseGet(Stream::empty).map(columnDesc -> getDbTblCols(tableDesc, columnDesc))
                        .flatMap(Set::stream).collect(Collectors.toSet()))
                .flatMap(Set::stream).collect(Collectors.toSet());
    }

    private Set<String> getDbTblCols(TableDesc tableDesc, ColumnDesc columnDesc) {
        Set<String> result = Sets.newHashSet();
        if (columnDesc.isComputedColumn()) {
            result.addAll(ComputedColumnUtil.getCCUsedColsWithProject(project, columnDesc));
        } else {
            result.add(String.format(IDENTIFIER_FORMAT, tableDesc.getIdentity(), columnDesc.getName()));
        }
        return result;
    }

    private Map<String, Set<String>> getAuthorizedTableColumns(List<AclTCR> all) {
        Map<String, Set<String>> authorizedCoarse = Maps.newHashMap();
        all.stream().forEach(aclTCR -> aclTCR.getTable().forEach((dbTblName, columnRow) -> {
            if (authorizedCoarse.containsKey(dbTblName) && Objects.isNull(authorizedCoarse.get(dbTblName))) {
                return;
            }
            if (Objects.isNull(columnRow) || Objects.isNull(columnRow.getColumn())) {
                //default table columns were authorized
                authorizedCoarse.put(dbTblName, null);
                return;
            }
            if (Objects.isNull(authorizedCoarse.get(dbTblName))) {
                authorizedCoarse.put(dbTblName, Sets.newHashSet());
            }
            authorizedCoarse.get(dbTblName).addAll(columnRow.getColumn());
        }));
        return authorizedCoarse;
    }
}
