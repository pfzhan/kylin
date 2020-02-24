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
import org.apache.kylin.rest.service.BasicService;
import org.apache.kylin.rest.util.AclEvaluate;
import org.apache.kylin.rest.util.AclPermissionUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.clearspring.analytics.util.Lists;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.transaction.UnitOfWork;
import io.kyligence.kap.metadata.acl.AclTCR;
import io.kyligence.kap.metadata.acl.AclTCRManager;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.UnitOfAllWorks;
import io.kyligence.kap.metadata.user.NKylinUserManager;
import io.kyligence.kap.rest.request.AccessRequest;
import io.kyligence.kap.rest.request.AclTCRRequest;
import io.kyligence.kap.rest.response.AclTCRResponse;
import io.kyligence.kap.rest.transaction.Transaction;
import lombok.val;

@Component("aclTCRService")
public class AclTCRService extends BasicService {

    private static final Logger logger = LoggerFactory.getLogger(AclTCRService.class);

    private static final String IDENTIFIER_FORMAT = "%s.%s";

    @Autowired
    private AclEvaluate aclEvaluate;

    public void revokeAclTCR(String uuid, String sid, boolean principal) {
        // permission already has been checked in AccessService#revokeAcl
        getProjectManager().listAllProjects().stream().filter(p -> p.getUuid().equals(uuid)).findFirst()
                .ifPresent(prj -> UnitOfWork.doInTransactionWithRetry(() -> {
                    revokePrjAclTCR(prj.getName(), sid, principal);
                    return null;
                }, prj.getName()));
    }

    public void revokeAclTCR(String sid, boolean principal) {
        // only global admin has permission
        // permission already has been checked in UserController, UserGroupController
        UnitOfAllWorks.doInTransaction(() -> {
            getProjectManager().listAllProjects().forEach(prj -> revokePrjAclTCR(prj.getName(), sid, principal));
            return null;
        }, false);
    }

    private void revokePrjAclTCR(String project, String sid, boolean principal) {
        logger.info("revoke project table, column and row acls of project={}, sid={}, principal={}", project, sid,
                principal);
        getAclTCRManager(project).revokeAclTCR(sid, principal);
    }

    @Transaction(project = 0)
    public void unloadTable(String project, String dbTblName) {
        getAclTCRManager(project).unloadTable(dbTblName);
    }

    public List<AclTCRResponse> getAclTCRResponse(String project, String sid, boolean principal,
            boolean authorizedOnly) {
        aclEvaluate.checkProjectAdminPermission(project);
        AclTCRManager aclTCRManager = getAclTCRManager(project);
        AclTCR authorized = aclTCRManager.getAclTCR(sid, principal);
        if (Objects.isNull(authorized)) {
            return Lists.newArrayList();
        }
        if (Objects.isNull(authorized.getTable())) {
            //default all tables were authorized
            return getAclTCRResponse(aclTCRManager.getAllDbAclTable(project));
        }
        if (authorizedOnly) {
            return tagTableNum(getAclTCRResponse(aclTCRManager.getDbAclTable(project, authorized)),
                    getDbTblColNum(project));
        }
        //all tables with authorized tcr tagged
        return getAclTCRResponse(project, aclTCRManager.getDbAclTable(project, authorized));
    }

    public void updateAclTCR(String project, String sid, boolean principal, List<AclTCRRequest> requests) {
        aclEvaluate.checkProjectAdminPermission(project);
        UnitOfWork.doInTransactionWithRetry(() -> {
            updateAclTCR(project, sid, principal, transformRequests(project, requests));
            return null;
        }, project);
    }

    public void updateAclTCR(String uuid, List<AccessRequest> requests) {
        // permission already has been checked in AccessService#grant, batchGrant
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

    private void updateAclTCR(String project, String sid, boolean principal, AclTCR aclTCR) {
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
            tbl.setAuthorized(false);
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
        return getAclTCRManager(project).getAllDbAclTable(project).entrySet().stream().map(de -> {
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

    @VisibleForTesting
    NKylinUserManager getKylinUserManager() {
        return NKylinUserManager.getInstance(getConfig());
    }

    public List<TableDesc> getAuthorizedTables(String project, String user) {
        Set<String> groups = getKylinUserManager().getUserGroups(user);
        return getAuthorizedTables(project, user, groups);
    }

    @VisibleForTesting
    NTableMetadataManager getTableMetadataManager(String project) {
        Preconditions.checkNotNull(project);
        return NTableMetadataManager.getInstance(getConfig(), project);
    }

    @VisibleForTesting
    boolean canUseACLGreenChannel(String project) {
        return AclPermissionUtil.canUseACLGreenChannel(project);
    }

    @VisibleForTesting
    List<TableDesc> getAuthorizedTables(String project, String user, Set<String> groups) {
        if (canUseACLGreenChannel(project)) {
            return getTableMetadataManager(project).listAllTables();
        }

        List<AclTCR> aclTCRS = getAclTCRManager(project).getAclTCRs(user, groups);
        return getTableMetadataManager(project).listAllTables().stream()
                .filter(tableDesc -> aclTCRS.stream().anyMatch(aclTCR -> aclTCR.isAuthorized(tableDesc.getIdentity())))
                .collect(Collectors.toList());
    }
}
