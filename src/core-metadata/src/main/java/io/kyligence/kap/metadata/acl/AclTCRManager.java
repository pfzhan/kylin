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
import io.kyligence.kap.common.scheduler.SchedulerEventBusFactory;
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
            aclTCR.getTable().remove(dbTblName);
            userCrud.save(aclTCR);
        });

        groupCrud.listAll().forEach(aclTCR -> {
            if (Objects.isNull(aclTCR.getTable())) {
                return;
            }
            aclTCR.getTable().remove(dbTblName);
            groupCrud.save(aclTCR);
        });
        postAclChangeEvent(null);
    }

    public AclTCR getAclTCR(String sid, boolean principal) {
        if (principal) {
            return userCrud.get(sid);
        }
        return groupCrud.get(sid);
    }

    public void updateAclTCR(AclTCR aclTCR, String sid, boolean principal) {
        aclTCR.init(sid);
        if (principal) {
            userCrud.save(aclTCR);
        } else {
            groupCrud.save(aclTCR);
        }
        postAclChangeEvent(sid);
    }

    public void revokeAclTCR(String sid, boolean principal) {
        if (principal) {
            userCrud.delete(sid);
        } else {
            groupCrud.delete(sid);
        }
        postAclChangeEvent(sid);
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
        final List<AclTCR> all = getAclTCRs(username, groups);
        if (isTablesAuthorized(all)) {
            return Maps.newHashMap();
        }
        Map<String, Map<String, Set<String>>> dbTblColRow = Maps.newHashMap();
        all.forEach(aclTCR -> aclTCR.getTable().forEach((dbTblName, columnRow) -> {
            if (dbTblColRow.containsKey(dbTblName) && Objects.isNull(dbTblColRow.get(dbTblName))) {
                return;
            }
            if (Objects.isNull(columnRow) || Objects.isNull(columnRow.getRow()) || columnRow.getRow().entrySet()
                    .stream().allMatch(entry -> CollectionUtils.isEmpty(entry.getValue()))) {
                dbTblColRow.put(dbTblName, null);
                return;
            }
            if (Objects.isNull(dbTblColRow.get(dbTblName))) {
                dbTblColRow.put(dbTblName, Maps.newHashMap());
            }
            getRows(dbTblName, columnRow, dbTblColRow);
        }));

        Map<String, String> result = Maps.newHashMap();
        dbTblColRow.entrySet().stream().filter(e -> MapUtils.isNotEmpty(e.getValue())).forEach(t -> {
            Map<String, String> columnType = Optional
                    .ofNullable(
                            NTableMetadataManager.getInstance(config, project).getTableDesc(t.getKey()).getColumns())
                    .map(Arrays::stream).orElseGet(Stream::empty)
                    .map(columnDesc -> new AbstractMap.SimpleEntry<>(columnDesc.getName(), columnDesc.getTypeName()))
                    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
            ColumnToConds columnToConds = new ColumnToConds();
            t.getValue().entrySet().stream().filter(e -> CollectionUtils.isNotEmpty(e.getValue()))
                    .forEach(c -> columnToConds.put(c.getKey(),
                            c.getValue().stream().map(ColumnToConds.Cond::new).collect(Collectors.toList())));
            if (MapUtils.isNotEmpty(columnToConds)) {
                result.put(t.getKey(), ColumnToConds.concatConds(columnToConds, columnType));
            }
        });

        return result;
    }

    private void getRows(String dbTblName, AclTCR.ColumnRow columnRow,
            Map<String, Map<String, Set<String>>> dbTblColRow) {
        columnRow.getRow().forEach((colName, realRow) -> {
            if (dbTblColRow.get(dbTblName).containsKey(colName)
                    && Objects.isNull(dbTblColRow.get(dbTblName).get(colName))) {
                return;
            }
            if (CollectionUtils.isEmpty(realRow)) {
                dbTblColRow.get(dbTblName).put(colName, null);
                return;
            }
            if (Objects.isNull(dbTblColRow.get(dbTblName).get(colName))) {
                dbTblColRow.get(dbTblName).put(colName, Sets.newHashSet());
            }
            dbTblColRow.get(dbTblName).get(colName).addAll(realRow);
        });
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

    private void postAclChangeEvent(String sid) {
        SchedulerEventBusFactory.getInstance(config).post(new AclTCR.ChangeEvent(project, sid));
    }
}
