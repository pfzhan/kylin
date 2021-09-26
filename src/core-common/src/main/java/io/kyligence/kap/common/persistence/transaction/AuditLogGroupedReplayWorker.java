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
package io.kyligence.kap.common.persistence.transaction;

import static io.kyligence.kap.common.persistence.metadata.jdbc.JdbcUtil.withTransaction;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.ResourceStore;
import org.springframework.transaction.TransactionException;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;

import io.kyligence.kap.common.persistence.AuditLog;
import io.kyligence.kap.common.persistence.metadata.JdbcAuditLogStore;
import lombok.AllArgsConstructor;
import lombok.val;
import lombok.var;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class AuditLogGroupedReplayWorker extends AbstractAuditLogReplayWorker {

    private static final String GLOBAL_PROJECT = UnitOfWork.GLOBAL_UNIT;

    ConcurrentHashMap<String, Long> project2Offset = new ConcurrentHashMap<>();

    public AuditLogGroupedReplayWorker(KylinConfig config, JdbcAuditLogStore auditLogStore) {
        super(config, auditLogStore);
    }

    public void startSchedule(long currentId, boolean needSync) {
        initProject(currentId);
        if (needSync) {
            catchupInternal(1);
        }
        long interval = config.getCatchUpInterval();
        consumeExecutor.scheduleWithFixedDelay(() -> catchupInternal(1), interval, interval, TimeUnit.SECONDS);
    }

    private synchronized void initProject(long currentId) {
        val store = ResourceStore.getKylinMetaStore(config);
        Set<String> projects = extractProjects(store);
        project2Offset.clear();
        projects.forEach(project -> project2Offset.put(project, currentId));
    }

    private Set<String> extractProjects(ResourceStore store) {
        Set<String> resourcePaths = store.listResources(ResourceStore.PROJECT_ROOT);
        Set<String> projects = Sets.newHashSet();
        if (resourcePaths != null) {
            resourcePaths.forEach(resourcePath -> projects.add(extractProjectName(resourcePath)));
        }
        projects.add(GLOBAL_PROJECT);
        return projects;
    }

    private String extractProjectName(String resourcePath) {
        return resourcePath.substring(ResourceStore.PROJECT_ROOT.length() + 1,
                resourcePath.length() - ".json".length());
    }

    private synchronized void resetProjectOffset(long offset, boolean force) {
        log.trace("reset offset: {}", offset);
        log.trace("before resetting projectOffset: {}", project2Offset);
        val store = ResourceStore.getKylinMetaStore(config);
        Set<String> existProjects = extractProjects(store);
        Map<String, Long> newProject2Offset = Maps.newHashMap();
        existProjects.forEach(project -> {
            long newOffset = force ? offset : Math.max(project2Offset.getOrDefault(project, 0L), offset);
            newProject2Offset.put(project, newOffset);
        });
        project2Offset.clear();
        project2Offset.putAll(newProject2Offset);
        log.trace("after resetting projectOffset: {}", project2Offset);
    }

    // not exactly log offset
    @Override
    public long getLogOffset() {
        return project2Offset.values().stream().min(Comparator.naturalOrder()).orElse(0L);
    }

    private long getMaxLogOffset() {
        return project2Offset.values().stream().max(Comparator.naturalOrder()).orElse(0L);
    }

    @Override
    public void updateOffset(long expected) {
        resetProjectOffset(expected, false);
    }

    @Override
    public void forceUpdateOffset(long expected) {
        resetProjectOffset(expected, true);
    }

    @Override
    protected boolean hasCatch(long targetId) {
        return getMaxLogOffset() >= targetId;
    }

    @Override
    protected synchronized void catchupInternal(int countDown) {
        if (isStopped.get()) {
            log.info("Catchup Already stopped");
            return;
        }
        log.debug("start replay...");
        val startTime = System.currentTimeMillis();

        Map<String, Long> replayInfoCollector = new HashMap<>();
        List<ProjectChangeEvent> projectChangeEvents = Lists.newArrayList();
        boolean nonSkipException = true;

        for (Entry<String, Long> entry : project2Offset.entrySet()) {
            val project = entry.getKey();
            val offset = entry.getValue();
            try {
                replayInfoCollector.put(project, catchupForProject(project, offset, projectChangeEvents));
            } catch (TransactionException e) {
                log.warn("cannot create transaction, ignore it", e);
                nonSkipException = false;
                break;
            } catch (Exception e) {
                handleReloadAll(e);
                return;
            }
        }

        long maxOffset = getMaxLogOffset();
        long minOffset = getLogOffset();
        boolean needOptimizeOffset = nonSkipException && canOptimizeOffset(minOffset, maxOffset);
        replayInfoCollector.forEach((project, offset) -> {
            long newOffset = needOptimizeOffset ? Math.max(maxOffset, offset) : offset;
            logProjectOffsetChange(project, offset, newOffset);
            project2Offset.put(project, newOffset);
        });
        handleProjectChange(projectChangeEvents);
        log.debug("replay spend {}ms", System.currentTimeMillis() - startTime);
    }

    private void logProjectOffsetChange(String project, long offset, long newOffset) {
        long oldOffset = project2Offset.get(project);
        if (oldOffset != newOffset) {
            log.debug("restore project {}, replay start offset {} to offset {}, and optimize to {} ", project,
                    project2Offset.get(project), offset, newOffset);
        }
    }

    private long catchupForProject(String project, Long startOffset, List<ProjectChangeEvent> projectChangeEvents) {
        val replayer = MessageSynchronization.getInstance(config);
        val store = ResourceStore.getKylinMetaStore(config);
        replayer.setChecker(store.getChecker());
        val projectMaxId = auditLogStore.getMaxIdByProject(project, startOffset);

        if (projectMaxId != 0) {
            withTransaction(auditLogStore.getTransactionManager(), () -> {
                var start = startOffset;
                boolean needDetectProjectChange = project.equals(GLOBAL_PROJECT);

                while (start < projectMaxId) {
                    val logs = auditLogStore.fetch(project, start, Math.min(STEP, projectMaxId - start));
                    if (needDetectProjectChange) {
                        collectProjectChange(logs, projectChangeEvents);
                    }
                    replayLogs(replayer, logs);
                    start += STEP;
                }
                return projectMaxId;
            });
            return projectMaxId;
        }

        return startOffset;
    }

    private boolean canOptimizeOffset(long minOffset, long maxOffset) {
        return maxOffset - minOffset > STEP * 10 && logAllCommit(minOffset, maxOffset);
    }

    private synchronized void handleProjectChange(List<ProjectChangeEvent> projectChangeEvents) {
        projectChangeEvents.forEach(changeEvent -> {
            if (changeEvent.isDelete) {
                project2Offset.remove(changeEvent.project);
            } else {
                project2Offset.put(changeEvent.project, changeEvent.id);
            }
        });
    }

    private void collectProjectChange(List<AuditLog> logs, List<ProjectChangeEvent> projectChangeEvents) {
        for (AuditLog log : logs) {
            if (log.getResPath().startsWith(ResourceStore.PROJECT_ROOT)) {
                // delete project
                if (log.getByteSource() == null) {
                    projectChangeEvents
                            .add(new ProjectChangeEvent(log.getId(), extractProjectName(log.getResPath()), true));
                } else if (log.getMvcc() == 0) {
                    // create project
                    projectChangeEvents
                            .add(new ProjectChangeEvent(log.getId(), extractProjectName(log.getResPath()), false));
                }
            }
        }

    }

    @AllArgsConstructor
    private static class ProjectChangeEvent {
        long id;
        String project;
        boolean isDelete;
    }

}
