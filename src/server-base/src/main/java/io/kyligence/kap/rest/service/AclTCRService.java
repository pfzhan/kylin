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

package io.kyligence.kap.rest.service;

import java.util.AbstractMap;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KapConfig;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.rest.service.BasicService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Component;

import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.rest.request.AccessRequest;
import io.kyligence.kap.rest.request.AclTCRRequest;
import io.kyligence.kap.rest.response.AclTCRResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

@Component("aclTCRService")
public class AclTCRService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(AclTCRService.class);

    private static final String IDENTIFIER_FORMAT = "%s.%s";

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void revokeAclTCR(String uuid, String sid, boolean principal) {
        getProjectManager().listAllProjects().stream().filter(p -> p.getUuid().equals(uuid)).findFirst()
                .ifPresent(prj -> UnitOfWork.doInTransactionWithRetry(() -> {
                    logger.info("revoke acl tcr project={}, sid={}, principal={}", prj.getName(), sid, principal);
                    getAclTCRManager(prj.getName()).revokeAclTCR(sid, principal);
                    return null;
                }, prj.getName()));
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void revokeAclTCR(String sid, boolean principal) {
        getProjectManager().listAllProjects().forEach(prj -> UnitOfWork.doInTransactionWithRetry(() -> {
            logger.info("revoke acl tcr project={}, sid={}, principal={}", prj.getName(), sid, principal);
            getAclTCRManager(prj.getName()).revokeAclTCR(sid, principal);
            return null;
        }, prj.getName()));
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN + " or hasPermission(#project, 'ADMINISTRATION')")
    @Transaction(project = 0)
    public void unloadTable(String project, String dbTblName) {
        getAclTCRManager(project).unloadTable(dbTblName);
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public List<AclTCRResponse> getAclTCRResponse(String project, String sid, boolean principal,
            boolean authorizedOnly) {
        AclTCR authorized = getAclTCRManager(project).getAclTCR(sid, principal);
        if (Objects.isNull(authorized)) {
            return Lists.newArrayList();
        }
        if (Objects.isNull(authorized.getTable())) {
            //default all tables were authorized
            return getAclTCRResponse(getAllDbAclTable(project));
        }
        if (authorizedOnly) {
            return tagTableNum(getAclTCRResponse(getDbAclTable(project, authorized)), getDbTblColNum(project));
        }
        //all tables with authorized tcr tagged
        return getAclTCRResponse(project, getDbAclTable(project, authorized));
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    @Transaction(project = 0)
    public void updateAclTCR(String project, String sid, boolean principal, List<AclTCRRequest> requests) {
        updateAclTCR(project, sid, principal, transformRequests(project, requests));
    }

    @PreAuthorize(Constant.ACCESS_HAS_ROLE_ADMIN)
    public void updateAclTCR(String uuid, List<AccessRequest> requests) {
        final boolean defaultAuthorized = KapConfig.getInstanceFromEnv().isProjectInternalDefaultPermissionGranted();
        getProjectManager().listAllProjects().stream().filter(p -> p.getUuid().equals(uuid)).findFirst()
                .ifPresent(prj -> UnitOfWork.doInTransactionWithRetry(() -> {
                    requests.stream().filter(r -> StringUtils.isNotEmpty(r.getSid())).forEach(r -> {
                        AclTCR aclTCR = new AclTCR();
                        if (!defaultAuthorized) {
                            aclTCR.setTable(new AclTCR.Table());
                        }
                        updateAclTCR(prj.getName(), r.getSid(), r.isPrincipal(), aclTCR);
                    });
                    return null;
                }, prj.getName()));
    }

    private void updateAclTCR(String project, String sid, boolean principal, AclTCR updateTo) {
        AclTCR aclTCR = getAclTCRManager(project).getAclTCR(sid, principal);
        if (aclTCR == null) {
            aclTCR = updateTo;
        } else {
            aclTCR.setTable(updateTo.getTable());
        }
        getAclTCRManager(project).updateAclTCR(aclTCR, sid, principal);
    }

    private List<AclTCRResponse.Row> getRows(AclTCR.ColumnRow authorizedColumnRow) {
        if (Objects.isNull(authorizedColumnRow) || Objects.isNull(authorizedColumnRow.getRow())) {
            return Lists.newArrayList();
        }
        return transformResponseRow(authorizedColumnRow.getRow());
    }

    private List<AclTCRResponse.Column> getColumns(AclTCR.ColumnRow columnRow, boolean isTableAuthorized,
            AclTCR.ColumnRow authorizedColumnRow) {
        if (Objects.isNull(columnRow) || Objects.isNull(columnRow.getColumn())) {
            return Lists.newArrayList();
        }
        boolean isNull = Objects.isNull(authorizedColumnRow);
        return columnRow.getColumn().stream().map(colName -> {
            AclTCRResponse.Column col = new AclTCRResponse.Column();
            col.setColumnName(colName);
            col.setAuthorized(false);
            if (isTableAuthorized && (isNull || Objects.isNull(authorizedColumnRow.getColumn()))) {
                col.setAuthorized(true);
            } else if (!isNull && Objects.nonNull(authorizedColumnRow.getColumn())) {
                col.setAuthorized(authorizedColumnRow.getColumn().contains(colName));
            }
            return col;
        }).collect(Collectors.toList());
    }

    private List<AclTCRResponse.Table> getTables(AclTCR.Table table, final AclTCR.Table authorizedTable) {
        if (Objects.isNull(table)) {
            return Lists.newArrayList();
        }
        final boolean nonNull = Objects.nonNull(authorizedTable);
        return table.entrySet().stream().map(te -> {
            AclTCRResponse.Table tbl = new AclTCRResponse.Table();
            tbl.setTableName(te.getKey());
            tbl.setAuthorized(!nonNull);
            AclTCR.ColumnRow authorizedColumnRow = null;
            if (nonNull) {
                tbl.setAuthorized(authorizedTable.containsKey(te.getKey()));
                authorizedColumnRow = authorizedTable.get(te.getKey());
            }

            val columns = getColumns(te.getValue(), tbl.isAuthorized(), authorizedColumnRow);
            tbl.setTotalColumnNum(columns.size());
            tbl.setAuthorizedColumnNum(
                    columns.stream().filter(AclTCRResponse.Column::isAuthorized).mapToInt(i -> 1).sum());
            tbl.setColumns(columns);
            tbl.setRows(getRows(authorizedColumnRow));
            return tbl;
        }).collect(Collectors.toList());
    }

    private List<AclTCRResponse> getAclTCRResponse(String project, final TreeMap<String, AclTCR.Table> authorized) {
        return getAllDbAclTable(project).entrySet().stream().map(de -> {
            AclTCRResponse response = new AclTCRResponse();
            response.setDatabaseName(de.getKey());
            response.setAuthorizedTableNum(
                    Objects.isNull(authorized.get(de.getKey())) ? 0 : authorized.get(de.getKey()).size());
            response.setTotalTableNum(de.getValue().size());
            response.setTables(getTables(de.getValue(), authorized.get(de.getKey())));
            return response;
        }).collect(Collectors.toList());
    }

    private List<AclTCRResponse> getAclTCRResponse(TreeMap<String, AclTCR.Table> db2AclTable) {
        return db2AclTable.entrySet().stream().map(de -> {
            AclTCRResponse response = new AclTCRResponse();
            response.setDatabaseName(de.getKey());
            response.setAuthorizedTableNum(de.getValue().size());
            response.setTotalTableNum(de.getValue().size());
            response.setTables(de.getValue().entrySet().stream().map(te -> {
                AclTCRResponse.Table tbl = new AclTCRResponse.Table();
                tbl.setTableName(te.getKey());
                tbl.setAuthorized(true);
                tbl.setTotalColumnNum(te.getValue().getColumn().size());
                tbl.setAuthorizedColumnNum(te.getValue().getColumn().size());
                tbl.setColumns(te.getValue().getColumn().stream().map(colName -> {
                    AclTCRResponse.Column col = new AclTCRResponse.Column();
                    col.setColumnName(colName);
                    col.setAuthorized(true);
                    return col;
                }).collect(Collectors.toList()));
                tbl.setRows(transformResponseRow(te.getValue().getRow()));
                return tbl;
            }).collect(Collectors.toList()));
            return response;
        }).collect(Collectors.toList());
    }

    private List<AclTCRResponse.Row> transformResponseRow(AclTCR.Row aclRow) {
        if (MapUtils.isEmpty(aclRow)) {
            return Lists.newArrayList();
        }
        return aclRow.entrySet().stream().filter(e -> Objects.nonNull(e.getValue())).map(entry -> {
            AclTCRResponse.Row row = new AclTCRResponse.Row();
            row.setColumnName(entry.getKey());
            row.setItems(Lists.newArrayList(entry.getValue()));
            return row;
        }).collect(Collectors.toList());
    }

    private TreeMap<String, AclTCR.Table> getDbAclTable(String project, AclTCR authorized) {
        if (Objects.isNull(authorized) || Objects.isNull(authorized.getTable())) {
            return Maps.newTreeMap();
        }
        TreeMap<String, AclTCR.Table> db2AclTable = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        authorized.getTable().forEach((dbTblName, cr) -> {
            TableDesc tableDesc = getTableManager(project).getTableDesc(dbTblName);
            if (!db2AclTable.containsKey(tableDesc.getDatabase())) {
                db2AclTable.put(tableDesc.getDatabase(), new AclTCR.Table());
            }

            if (Objects.isNull(cr) || Objects.isNull(cr.getColumn())) {
                AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
                AclTCR.Column aclColumn = new AclTCR.Column();
                aclColumn.addAll(getTableColumns(tableDesc));
                columnRow.setColumn(aclColumn);
                db2AclTable.get(tableDesc.getDatabase()).put(tableDesc.getName(), columnRow);
            } else {
                db2AclTable.get(tableDesc.getDatabase()).put(tableDesc.getName(), cr);
            }
        });
        return db2AclTable;
    }

    private List<String> getTableColumns(TableDesc t) {
        return Optional.ofNullable(t.getColumns()).map(Arrays::stream).orElseGet(Stream::empty).map(ColumnDesc::getName)
                .collect(Collectors.toList());
    }

    private TreeMap<String, AclTCR.Table> getAllDbAclTable(String project) {
        TreeMap<String, AclTCR.Table> db2AclTable = Maps.newTreeMap(String.CASE_INSENSITIVE_ORDER);
        getTableManager(project).listAllTables().stream()
                .filter(t -> StringUtils.isNotEmpty(t.getDatabase()) && StringUtils.isNotEmpty(t.getName()))
                .forEach(t -> {
                    AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
                    AclTCR.Column aclColumn = new AclTCR.Column();
                    aclColumn.addAll(getTableColumns(t));
                    columnRow.setColumn(aclColumn);
                    if (!db2AclTable.containsKey(t.getDatabase())) {
                        db2AclTable.put(t.getDatabase(), new AclTCR.Table());
                    }
                    db2AclTable.get(t.getDatabase()).put(t.getName(), columnRow);
                });
        return db2AclTable;
    }

    private List<AclTCRResponse> tagTableNum(List<AclTCRResponse> responses,
            Map<String, Map<String, Integer>> dbTblColNum) {
        responses.forEach(r -> {
            r.setTotalTableNum(dbTblColNum.get(r.getDatabaseName()).size());
            r.getTables().forEach(t -> t.setTotalColumnNum(dbTblColNum.get(r.getDatabaseName()).get(t.getTableName())));
        });
        return responses;
    }

    private void slim(String project, AclTCR aclTCR) {
        if (aclTCR == null || aclTCR.getTable() == null) {
            return;
        }

        aclTCR.getTable().forEach((dbTblName, columnRow) -> {
            if (Objects.isNull(columnRow)) {
                return;
            }

            if (Objects.nonNull(columnRow.getColumn())
                    && Optional.ofNullable(getTableManager(project).getTableDesc(dbTblName).getColumns())
                            .map(Arrays::stream).orElseGet(Stream::empty).map(ColumnDesc::getName)
                            .allMatch(colName -> columnRow.getColumn().contains(colName))) {
                columnRow.setColumn(null);
            }

            if (MapUtils.isEmpty(columnRow.getRow()) && Objects.isNull(columnRow.getColumn())) {
                aclTCR.getTable().put(dbTblName, null);
            }
        });

        if (getTableManager(project).listAllTables().stream().map(TableDesc::getIdentity)
                .allMatch(dbTblName -> aclTCR.getTable().containsKey(dbTblName))
                && aclTCR.getTable().entrySet().stream().allMatch(e -> Objects.isNull(e.getValue()))) {
            aclTCR.setTable(null);
        }
    }

    private AclTCR transformRequests(String project, List<AclTCRRequest> requests) {
        AclTCR aclTCR = new AclTCR();
        AclTCR.Table aclTable = new AclTCR.Table();
        requests.stream().filter(d -> StringUtils.isNotEmpty(d.getDatabaseName())).forEach(d -> d.getTables().stream()
                .filter(t -> t.isAuthorized() && StringUtils.isNotEmpty(t.getTableName())).forEach(t -> {
                    String dbTblName = String.format(IDENTIFIER_FORMAT, d.getDatabaseName(), t.getTableName());
                    AclTCR.ColumnRow columnRow = new AclTCR.ColumnRow();
                    AclTCR.Column aclColumn;
                    if (Optional.ofNullable(t.getColumns()).map(List::stream).orElseGet(Stream::empty)
                            .allMatch(AclTCRRequest.Column::isAuthorized)) {
                        aclColumn = null;
                    } else {
                        aclColumn = new AclTCR.Column();
                        aclColumn.addAll(Optional.ofNullable(t.getColumns()).map(List::stream).orElseGet(Stream::empty)
                                .filter(AclTCRRequest.Column::isAuthorized).map(AclTCRRequest.Column::getColumnName)
                                .collect(Collectors.toSet()));
                    }
                    columnRow.setColumn(aclColumn);

                    AclTCR.Row aclRow;
                    if (Optional.ofNullable(t.getRows()).map(List::stream).orElseGet(Stream::empty)
                            .allMatch(r -> CollectionUtils.isEmpty(r.getItems()))) {
                        aclRow = null;
                    } else {
                        aclRow = new AclTCR.Row();
                        t.getRows().stream().filter(r -> CollectionUtils.isNotEmpty(r.getItems())).map(
                                r -> new AbstractMap.SimpleEntry<>(r.getColumnName(), Sets.newHashSet(r.getItems())))
                                .collect(Collectors.<Map.Entry<String, HashSet<String>>, String, Set<String>> toMap(
                                        Map.Entry::getKey, Map.Entry::getValue, (v1, v2) -> {
                                            v1.addAll(v2);
                                            return v1;
                                        }))
                                .forEach((colName, rows) -> {
                                    AclTCR.RealRow realRow = new AclTCR.RealRow();
                                    realRow.addAll(rows);
                                    aclRow.put(colName, realRow);
                                });
                    }
                    columnRow.setRow(aclRow);

                    aclTable.put(dbTblName, columnRow);
                }));
        if (requests.stream().allMatch(d -> Optional.ofNullable(d.getTables()).map(List::stream)
                .orElseGet(Stream::empty).allMatch(AclTCRRequest.Table::isAuthorized))) {
            getTableManager(project).listAllTables().stream().filter(t -> !aclTable.containsKey(t.getIdentity()))
                    .map(TableDesc::getIdentity).forEach(dbTblName -> aclTable.put(dbTblName, null));
        }
        aclTCR.setTable(aclTable);
        slim(project, aclTCR);
        return aclTCR;
    }

    private Map<String, Map<String, Integer>> getDbTblColNum(String project) {
        Map<String, Map<String, Integer>> dbTblColNum = Maps.newHashMap();
        getTableManager(project).listAllTables().forEach(tableDesc -> {
            if (!dbTblColNum.containsKey(tableDesc.getDatabase())) {
                dbTblColNum.put(tableDesc.getDatabase(), Maps.newHashMap());
            }
            dbTblColNum.get(tableDesc.getDatabase()).put(tableDesc.getName(), tableDesc.getColumnCount());
        });
        return dbTblColNum;
    }
}
