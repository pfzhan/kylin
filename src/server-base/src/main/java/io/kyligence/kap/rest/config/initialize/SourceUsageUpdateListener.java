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
package io.kyligence.kap.rest.config.initialize;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.restclient.RestClient;
import org.apache.kylin.metadata.model.PartitionDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.apache.kylin.metadata.project.ProjectInstance;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.springframework.stereotype.Component;

import com.google.common.collect.Maps;

import io.kyligence.kap.common.scheduler.SourceUsageUpdateNotifier;
import io.kyligence.kap.common.scheduler.SourceUsageVerifyNotifier;
import io.kyligence.kap.engine.spark.utils.ThreadUtils;
import io.kyligence.kap.guava20.shaded.common.eventbus.Subscribe;
import io.kyligence.kap.metadata.cube.model.NDataflowManager;
import io.kyligence.kap.metadata.epoch.EpochManager;
import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NTableMetadataManager;
import io.kyligence.kap.metadata.project.EnhancedUnitOfWork;
import io.kyligence.kap.metadata.project.NProjectManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageManager;
import io.kyligence.kap.metadata.sourceusage.SourceUsageRecord;
import lombok.extern.slf4j.Slf4j;
import scala.Option;

@Component
@Slf4j
public class SourceUsageUpdateListener {

    private ConcurrentHashMap<String, RestClient> clientMap = new ConcurrentHashMap<>();

    private final ScheduledExecutorService scheduler = //
            ThreadUtils.newDaemonSingleThreadScheduledExecutor("sourceUsage");

    @Subscribe
    public void onUpdate(SourceUsageUpdateNotifier notifier) {
        KylinConfig kylinConfig = KylinConfig.getInstanceFromEnv();
        EpochManager epochManager = EpochManager.getInstance(kylinConfig);
        if (epochManager.checkEpochOwner(EpochManager.GLOBAL)) {
            log.debug("Start to update source usage...");
            SourceUsageManager sourceUsageManager = SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv());
            SourceUsageRecord sourceUsageRecord = sourceUsageManager.refreshLatestSourceUsageRecord();

            EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
                SourceUsageManager.getInstance(KylinConfig.getInstanceFromEnv()).updateSourceUsage(sourceUsageRecord);
                return 0;
            }, EpochManager.GLOBAL);
            log.info("Update source usage done: {}", sourceUsageRecord);
        } else {
            try {
                String owner = epochManager.getEpochOwner(EpochManager.GLOBAL).split("\\|")[0];
                if (clientMap.get(owner) == null) {
                    clientMap.clear();
                    clientMap.put(owner, new RestClient(owner));
                }
                log.debug("Start to notify {} to update source usage", owner);
                clientMap.get(owner).notify(notifier);
            } catch (Exception e) {
                log.error("Failed to update source usage using rest client", e);
            }
        }
    }

    @Subscribe
    public void onVerify(SourceUsageVerifyNotifier notifier) {
        log.debug("Verify model partition is aligned with source table partition.");
        KylinConfig config = KylinConfig.getInstanceFromEnv();
        final EpochManager epochManager = EpochManager.getInstance(config);
        List<String> projects = NProjectManager.getInstance(config).listAllProjects().stream() //
                .map(ProjectInstance::getName) //
                .filter(epochManager::checkEpochOwner) // project owner
                .collect(Collectors.toList());
        if (projects.isEmpty()) {
            log.debug("Skip verify source table partition, no projects.");
            scheduler.schedule(() -> onVerify(notifier), 30, TimeUnit.SECONDS);
            return;
        }
        verifyProjects(config, projects);
    }

    private void verifyProjects(KylinConfig config, List<String> projects) {
        boolean updatable = false;
        // shared among projects
        final Map<String, Boolean> colPartMap = Maps.newHashMap();
        SessionCatalog catalog = SparderEnv.getSparkSession().sessionState().catalog();
        for (String project : projects) {
            try {
                if (!verifyProject(config, project, catalog, colPartMap)) {
                    updatable = true;
                }
            } catch (Exception e) {
                log.warn("[UNEXPECTED_THINGS_HAPPENED] Verify project catalog table failed: {}", project, e);
            }
        }

        if (updatable) {
            log.debug("Source table partition recognized, update source usage.");
            onUpdate(new SourceUsageUpdateNotifier());
        }
    }

    private boolean verifyProject(KylinConfig config, String project, SessionCatalog catalog, //
            Map<String, Boolean> colPartMap) {
        NTableMetadataManager tableManager = NTableMetadataManager.getInstance(config, project);
        final List<TableDesc> copiedTables = NDataflowManager.getInstance(config, project).listUnderliningDataModels() //
                .stream().map(NDataModel::getPartitionDesc).filter(Objects::nonNull) //
                .map(PartitionDesc::getPartitionDateColumnRef).filter(Objects::nonNull) //
                .filter(ref -> !ref.getColumnDesc().isPartitioned()).distinct() // distinct
                .filter(colRef -> {
                    String canonical = colRef.getCanonicalName();
                    if (!colPartMap.containsKey(canonical)) {
                        colPartMap.put(canonical, isPartitioned(catalog, colRef));
                    }
                    return colPartMap.get(canonical);
                }).map(colRef -> {
                    TableDesc copied = tableManager.copyForWrite(colRef.getTableRef().getTableDesc());
                    Arrays.stream(copied.getColumns()) //
                            .filter(desc -> Objects.nonNull(desc.getName())) //
                            .filter(desc -> desc.getName().equals(colRef.getName())) //
                            .findAny().ifPresent(desc -> desc.setPartitioned(true));
                    return copied;
                }).collect(Collectors.toList());

        // persist
        EnhancedUnitOfWork.doInTransactionWithCheckAndRetry(() -> {
            for (TableDesc copied : copiedTables) {
                NTableMetadataManager.getInstance(KylinConfig.getInstanceFromEnv(), project) //
                        .updateTableDesc(copied);
            }
            return 0;
        }, project);

        return copiedTables.isEmpty();
    }

    private boolean isPartitioned(SessionCatalog catalog, TblColRef colRef) {
        try {
            String dbName = colRef.getTableRef().getTableDesc().getDatabase();
            TableIdentifier identifier = // 
                    TableIdentifier.apply(colRef.getTableRef().getTableName(), Option.apply(dbName));
            CatalogTable catalogTable = catalog.getTempViewOrPermanentTableMetadata(identifier);
            if (Objects.isNull(catalogTable) || Objects.isNull(catalogTable.partitionColumnNames())) {
                return false;
            }

            return scala.collection.JavaConverters //
                    .seqAsJavaListConverter(catalogTable.partitionColumnNames()).asJava() //
                    .stream().anyMatch(name -> name.equalsIgnoreCase(colRef.getName()));
        } catch (Exception e) {
            log.warn("[UNEXPECTED_THINGS_HAPPENED] Verify catalog table {} failed.", colRef.getTable(), e);
        }

        return false;
    }
}
