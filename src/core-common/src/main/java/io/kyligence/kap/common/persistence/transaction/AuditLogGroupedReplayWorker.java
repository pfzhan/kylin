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
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    private static final String GLOBAL_PROJECT = "_global";

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
        val store = ResourceStore.getKylinMetaStore(config);
        Set<String> existProjects = extractProjects(store);
        Map<String, Long> newProject2Offset = Maps.newHashMap();
        existProjects.forEach(project -> {
            long newOffset = force ? offset : Math.max(project2Offset.getOrDefault(project, 0L), offset);
            newProject2Offset.put(project, newOffset);
        });
        project2Offset.clear();
        project2Offset.putAll(newProject2Offset);
    }

    // not exactly log offset
    @Override
    public long getLogOffset() {
        return project2Offset.values().stream().min(Comparator.naturalOrder()).orElse(0L);
    }

    @Override
    public void waitForCatchup(long targetId, long timeout) throws TimeoutException {

        long endTime = System.currentTimeMillis() + timeout * 1000;
        try {
            while (System.currentTimeMillis() < endTime) {
                if (getMaxLogOffset() >= targetId) {
                    return;
                }
                Thread.sleep(50);
            }
        } catch (Exception e) {
            log.info("Wait for catchup to {} failed", targetId, e);
        }
        throw new TimeoutException(String.format(Locale.ROOT, "Cannot reach %s before %s, current is %s", targetId,
                endTime, getLogOffset()));
    }

    private long getMaxLogOffset() {
        return project2Offset.values().stream().max(Comparator.naturalOrder()).orElse(0L);
    }

    @Override
    public void updateOffset(long expected) {
        resetProjectOffset(expected, false);
        catchup();
    }

    @Override
    public void forceUpdateOffset(long expected) {
        resetProjectOffset(expected, true);
        catchup();
    }

    @Override
    public void forceCatchFrom(long expected) {
        forceUpdateOffset(expected);
        catchup();
    }

    @Override
    public void catchupFrom(long expected) {
        updateOffset(expected);
        catchup();
    }

    @Override
    protected synchronized void catchupInternal(int countDown) {
        if (isStopped.get()) {
            log.info("Catchup Already stopped");
            return;
        }
        try {
            val replayer = MessageSynchronization.getInstance(config);
            val store = ResourceStore.getKylinMetaStore(config);
            replayer.setChecker(store.getChecker());
            Map<String, Long> newOffsetCollector = Maps.newHashMap();
            List<ProjectChangeEvent> projectChangeEvents = Lists.newArrayList();

            long maxOffset = getMaxLogOffset();
            long minOffset = getLogOffset();
            boolean needOptimizeOffset = needOptimizeOffset(minOffset, maxOffset);

            project2Offset.entrySet().stream().parallel().forEach(entry -> {
                val project = entry.getKey();
                val offset = entry.getValue();
                val projectMaxId = auditLogStore.getMaxIdByProject(project, offset);
                log.debug(
                        "start restore project {}, current project max_id is {}(don't update if 0), current project id : {}",
                        project, projectMaxId, offset);
                if (projectMaxId != 0) {
                    withTransaction(auditLogStore.getTransactionManager(), () -> {
                        var start = offset;
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
                }

                if (needOptimizeOffset) {
                    newOffsetCollector.put(project, Math.max(projectMaxId, maxOffset));
                } else {
                    if (projectMaxId != 0) {
                        newOffsetCollector.put(project, projectMaxId);
                    }
                }

            });

            newOffsetCollector.forEach((project, newOffset) -> project2Offset.put(project, newOffset));

            handleProjectChange(projectChangeEvents);
        } catch (TransactionException e) {
            log.warn("cannot create transaction, ignore it", e);
            try {
                Thread.sleep(5000);
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
            }
        } catch (Exception e) {
            handleReloadAll(e);
        }
    }

    private boolean needOptimizeOffset(long minOffset, long maxOffset) {
        if (maxOffset - minOffset > STEP * 10 && logAllCommit(minOffset, maxOffset)) {
                return true;
        }
        return false;
    }

    private boolean logAllCommit(long startOffset, long endOffset) {
        return auditLogStore.count(startOffset, endOffset) == (endOffset - startOffset);
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
