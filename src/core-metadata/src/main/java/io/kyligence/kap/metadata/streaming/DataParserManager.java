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

package io.kyligence.kap.metadata.streaming;

import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_TABLES_USE_JAR;
import static org.apache.kylin.common.exception.code.ErrorCodeServer.CUSTOM_PARSER_TABLES_USE_PARSER;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.exception.KylinException;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.metadata.cachesync.CachedCrudAssist;

import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DataParserManager {

    private KylinConfig kylinConfig;
    private CachedCrudAssist<DataParserInfo> crud;

    private static final String DEFAULT_JAR = "default";
    private static final String DEFAULT_PARSER = "io.kyligence.kap.parser.TimedJsonStreamParser";

    public static DataParserManager getInstance(KylinConfig kylinConfig, String project) {
        return kylinConfig.getManager(project, DataParserManager.class);
    }

    static DataParserManager newInstance(KylinConfig kylinConfig, String project) {
        return new DataParserManager(kylinConfig, project);
    }

    private ResourceStore getStore() {
        return ResourceStore.getKylinMetaStore(this.kylinConfig);
    }

    private DataParserManager(KylinConfig kylinConfig, String project) {
        this.kylinConfig = kylinConfig;
        String resourceRootPath = String.format(Locale.ROOT, "/%s%s", project, ResourceStore.DATA_PARSER_RESOURCE_ROOT);
        this.crud = new CachedCrudAssist<DataParserInfo>(getStore(), resourceRootPath, DataParserInfo.class) {
            @Override
            protected DataParserInfo initEntityAfterReload(DataParserInfo entity, String resourceName) {
                return entity;
            }
        };
        crud.reloadAll();
    }

    public void initDefault(String project) {
        DataParserInfo defaultDataParser = new DataParserInfo(project, DEFAULT_PARSER, DEFAULT_JAR);
        if (!crud.contains(defaultDataParser.resourceName())) {
            createDataParserInfo(defaultDataParser);
        }
    }

    public DataParserInfo getDataParserInfo(String className) {
        if (org.apache.commons.lang.StringUtils.isEmpty(className)) {
            return null;
        }
        return crud.get(className);
    }

    public DataParserInfo createDataParserInfo(DataParserInfo dataParserInfo) {
        if (dataParserInfo == null || StringUtils.isEmpty(dataParserInfo.getClassName())) {
            throw new IllegalArgumentException("data parser info is null or class name is null");
        }
        if (crud.contains(dataParserInfo.resourceName())) {
            throw new IllegalArgumentException("data parser '" + dataParserInfo.getClassName() + "' already exists");
        }

        dataParserInfo.updateRandomUuid();
        return crud.save(dataParserInfo);
    }

    public DataParserInfo updateDataParserInfo(DataParserInfo dataParserInfo) {
        if (!crud.contains(dataParserInfo.resourceName())) {
            throw new IllegalArgumentException(
                    "DataParserInfo '" + dataParserInfo.resourceName() + "' does not exist.");
        }
        return crud.save(dataParserInfo);
    }

    /**
     * Delete a single parser, and throw an exception if a table is being referenced
     * For logical deletion, you can actually force load through reflection. Unless unregisterjar
     */
    public DataParserInfo removeParser(String className) {
        if (StringUtils.equals(className, DEFAULT_PARSER)) {
            throw new KylinException(CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER);
        }
        DataParserInfo dataParserInfo = getDataParserInfo(className);
        if (Objects.isNull(dataParserInfo)) {
            log.warn("Removing DataParserClass '{}' does not exist", className);
            return null;
        }
        if (!dataParserInfo.getStreamingTables().isEmpty()) {
            // There is a table reference, which cannot be deleted
            throw new KylinException(CUSTOM_PARSER_TABLES_USE_PARSER,
                    StringUtils.join(dataParserInfo.getStreamingTables(), ", "));
        }
        crud.delete(dataParserInfo);
        log.info("Removing DataParserClass '{}' success", className);
        return dataParserInfo;
    }

    public void removeJar(String jarPath) {
        if (StringUtils.equals(jarPath, DEFAULT_JAR)) {
            throw new KylinException(CUSTOM_PARSER_CANNOT_DELETE_DEFAULT_PARSER);
        }
        // Check whether all parsers under jar are referenced by table
        if (checkJarParserUse(jarPath)) {
            // There is a table reference, which cannot be deleted
            throw new KylinException(CUSTOM_PARSER_TABLES_USE_JAR,
                    StringUtils.join(getJarParserUseTable(jarPath), ","));
        }
        log.info("start to remove jar [{}]", jarPath);
        List<DataParserInfo> dataParserWithJar = getDataParserByJar(jarPath);
        dataParserWithJar.forEach(dataParserInfo -> removeParser(dataParserInfo.getClassName()));
    }

    public DataParserInfo removeUsingTable(String table, String className) {
        DataParserInfo dataParserInfo = getDataParserInfo(className);
        if (Objects.isNull(dataParserInfo)) {
            throw new IllegalArgumentException("data parser info is null");
        }
        dataParserInfo.getStreamingTables().remove(table);
        log.info("class [{}], remove using table [{}]", className, table);
        return updateDataParserInfo(dataParserInfo);
    }

    public List<DataParserInfo> listDataParserInfo() {
        return new ArrayList<>(crud.listAll());
    }

    public List<DataParserInfo> getDataParserByJar(String jarPath) {
        return crud.listAll().stream()
                .filter(dataParserInfo -> StringUtils.equalsIgnoreCase(dataParserInfo.getJarPath(), jarPath))
                .collect(Collectors.toList());
    }

    public boolean checkJarParserUse(String jarName) {
        return crud.listAll().stream()
                .anyMatch(dataParserInfo -> StringUtils.equalsIgnoreCase(dataParserInfo.getJarPath(), jarName)
                        && !dataParserInfo.getStreamingTables().isEmpty());
    }

    public List<String> getJarParserUseTable(String jarName) {
        List<String> tableList = Lists.newArrayList();
        crud.listAll().stream()
                .filter(dataParserInfo -> StringUtils.equalsIgnoreCase(dataParserInfo.getJarPath(), jarName)
                        && !dataParserInfo.getStreamingTables().isEmpty())
                .map(DataParserInfo::getStreamingTables).forEach(tableList::addAll);
        return tableList;
    }

}
