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
package io.kyligence.kap.rest.source;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.rest.constant.Constant;
import org.apache.kylin.source.ISourceMetadataExplorer;
import org.apache.kylin.source.SourceFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.annotations.VisibleForTesting;

import io.kyligence.kap.rest.response.NHiveTableNameResponse;

public class NHiveTableName implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(NHiveTableName.class);
    private static final NHiveTableName instance = new NHiveTableName();
    private Map<String, List<String>> tables;//Map<db,tables>
    private volatile boolean isRunning;
    private volatile long lastLoadTime;
    private final ISourceMetadataExplorer explr;
    private final ReentrantLock lock;

    public static NHiveTableName getInstance() {
        return instance;
    }

    private NHiveTableName() {
        this(SourceFactory.getSparkSource().getSourceMetadataExplorer());
    }

    private NHiveTableName(ISourceMetadataExplorer explr) {
        this.tables = null;
        this.isRunning = false;
        this.lastLoadTime = 0;
        this.explr = explr;
        this.lock = new ReentrantLock();
    }

    public NHiveTableNameResponse loadHiveTableName(boolean force) throws Exception {
        logger.warn("Call loadHiveTableName");
        boolean isAllNode = KylinConfig.getInstanceFromEnv().getServerMode().equals(Constant.SERVER_MODE_ALL);
        if (!isAllNode) {
            throw new RuntimeException("Only all node can load hive table name");
        }
        if (force && !isRunning && lock.tryLock()) {
            try {
                if (!isRunning) {
                    logger.info("Now start load hive table name");
                    isRunning = true;
                    long now = System.currentTimeMillis();
                    try {
                        Map<String, List<String>> newTables = new HashMap<>();
                        List<String> dbs = explr.listDatabases().stream().map(String::toUpperCase)
                                .collect(Collectors.toList());
                        for (String db : dbs) {
                            List<String> tbs = explr.listTables(db).stream().map(String::toUpperCase)
                                    .collect(Collectors.toList());
                            newTables.put(db, tbs);
                        }
                        setAllTables(newTables);
                        lastLoadTime = System.currentTimeMillis();
                    } catch (Exception e) {
                        logger.error("msg", e);
                    } finally {
                        isRunning = false;
                    }
                    long timeInterval = (System.currentTimeMillis() - now) / 1000;
                    logger.info(String.format("Load hive table name successful within %d second", timeInterval));
                } else {
                    logger.warn("No need to load hive table name because another thread is loading now");
                }
            } finally {
                lock.unlock();
            }
        } else {
            logger.warn("No need to load hive table name because another thread is doing now or force = false");
        }
        NHiveTableNameResponse response = new NHiveTableNameResponse();
        response.setIsRunning(isRunning);
        response.setTime(System.currentTimeMillis() - lastLoadTime);
        return response;
    }

    public synchronized List<String> getTables(String db) {
        List<String> result = null;
        if (tables != null) {
            result = tables.get(db);
        }
        return result != null ? result : new ArrayList<>();
    }

    private synchronized void setAllTables(Map<String, List<String>> newTables) {
        tables = newTables;
    }

    @VisibleForTesting
    public synchronized void setAllTablesForTest(Map<String, List<String>> newTables) {
        tables = newTables;
    }

    @Override
    public void run() {
        try {
            loadHiveTableName(true);
        } catch (Exception e) {
            logger.error("msg", e);
        }
    }
}
