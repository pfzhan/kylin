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
package io.kyligence.kap.engine.spark.source;

import java.io.Serializable;
import java.net.URI;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.collect.Sets;
import org.apache.commons.jnet.Installer;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.CatalogTableType;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import scala.Option;
import scala.collection.JavaConversions;

public class NSparkTableMetaExplorer implements Serializable {

    private static final Logger logger = LoggerFactory.getLogger(NSparkTableMetaExplorer.class);

    enum PROVIDER {
        HIVE("hive"), UNSPECIFIED("");

        private static final PROVIDER[] ALL = new PROVIDER[] { HIVE };
        private String value;

        PROVIDER(String value) {
            this.value = value;
        }

        public static PROVIDER fromString(Option<String> value) {
            if (value.isEmpty()) {
                return UNSPECIFIED;
            }

            for (PROVIDER provider : ALL) {
                if (provider.value.equals(value.get())) {
                    return provider;
                }
            }
            return UNSPECIFIED;
        }
    }

    private static final List<String> UNSUPOORT_TYPE = Lists.newArrayList("array", "map", "struct", "binary");
    private static final String CHAR_VARCHAR_TYPE_STRING_METADATA_KEY = "__CHAR_VARCHAR_TYPE_STRING";

    public NSparkTableMeta getSparkTableMeta(String database, String tableName) {
        SessionCatalog catalog = SparderEnv.getSparkSession().sessionState().catalog();
        TableIdentifier tableIdentifier = TableIdentifier.apply(tableName,
                Option.apply(database.isEmpty() ? null : database));
        CatalogTable tableMetadata = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier);
        checkTableIsValid(tableMetadata, tableIdentifier, tableName);
        return getSparkTableMeta(tableName, tableMetadata);
    }

    public Set<String> checkAndGetTablePartitions(String database, String tableName, String partitionCol) {
        SessionCatalog catalog = SparderEnv.getSparkSession().sessionState().catalog();
        TableIdentifier tableIdentifier = TableIdentifier.apply(tableName,
                Option.apply(database.isEmpty() ? null : database));

        CatalogTable tableMetadata = catalog.getTempViewOrPermanentTableMetadata(tableIdentifier);

        String firstPartCol = tableMetadata.partitionColumnNames().isEmpty() ? null
                : tableMetadata.partitionColumnNames().head().toLowerCase(Locale.ROOT);

        if (!partitionCol.equalsIgnoreCase(firstPartCol)) {
            throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "table partition col %s not match col %s", firstPartCol, partitionCol));
        }
        return JavaConversions.seqAsJavaList(catalog.listPartitions(tableIdentifier, Option.empty())).stream()
                .map(item -> JavaConversions.mapAsJavaMap(item.spec()).entrySet().stream()
                        .filter(entry -> partitionCol.equalsIgnoreCase(entry.getKey())) //
                        .findFirst() //
                        .map(Map.Entry::getValue) //
                        .orElse(null))
                .filter(Objects::nonNull).collect(Collectors.toSet());
    }

    private NSparkTableMeta getSparkTableMeta(String tableName, CatalogTable tableMetadata) {
        NSparkTableMetaBuilder builder = new NSparkTableMetaBuilder();
        builder.setTableName(tableName);
        builder.setAllColumns(getColumns(tableMetadata, tableMetadata.schema()));
        builder.setOwner(tableMetadata.owner());
        builder.setCreateTime(tableMetadata.createTime() + "");
        builder.setLastAccessTime(tableMetadata.lastAccessTime() + "");
        builder.setTableType(tableMetadata.tableType().name());
        builder.setPartitionColumns(getColumns(tableMetadata, tableMetadata.partitionSchema()));
        builder.setIsRangePartition(isRangePartition(tableMetadata));
        if (tableMetadata.storage().inputFormat().isDefined()) {
            builder.setSdInputFormat(tableMetadata.storage().inputFormat().get());
        }
        if (tableMetadata.storage().outputFormat().isDefined()) {
            builder.setSdOutputFormat(tableMetadata.storage().outputFormat().get());
        }
        Option<URI> uriOption = tableMetadata.storage().locationUri();
        if (uriOption.isDefined()) {
            builder.setSdLocation(uriOption.get().toString());
        }
        if (tableMetadata.provider().isDefined()) {
            builder.setProvider(tableMetadata.provider().get());
        }
        if (tableMetadata.properties().contains("totalSize")) {
            builder.setFileSize(Long.parseLong(tableMetadata.properties().get("totalSize").get()));
        }
        if (tableMetadata.properties().contains("numFiles")) {
            builder.setFileNum(Long.parseLong(tableMetadata.properties().get("numFiles").get()));
        }
        if (tableMetadata.properties().contains("transactional")) {
            builder.setIsTransactional(Boolean.parseBoolean(tableMetadata.properties().get("transactional").get()));
        }
        return builder.createSparkTableMeta();
    }

    private List<NSparkTableMeta.SparkTableColumnMeta> getColumns(CatalogTable tableMetadata, StructType schema) {
        return getColumns(tableMetadata, schema, true);
    }

    private List<NSparkTableMeta.SparkTableColumnMeta> getColumns(CatalogTable tableMetadata, StructType schema,
                                                                  boolean isCheckRepeatColumn) {
        List<NSparkTableMeta.SparkTableColumnMeta> allColumns = Lists.newArrayListWithCapacity(schema.size());
        Set<String> columnCacheTemp = Sets.newHashSet();
        for (org.apache.spark.sql.types.StructField field : schema.fields()) {
            String type = field.dataType().simpleString();
            if (field.metadata().contains(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY)) {
                type = field.metadata().getString(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY);
            }
            String finalType = type;
            if (UNSUPOORT_TYPE.stream().anyMatch(finalType::contains)) {
                logger.info("Load table {} ignore column {}:{}", tableMetadata.identifier().identifier(), field.name(),
                        finalType);
                continue;
            }
            if(isCheckRepeatColumn && columnCacheTemp.contains(field.name())) {
                logger.info("The【{}】column is already included and does not need to be added again", field.name());
                continue;
            }
            columnCacheTemp.add(field.name());
            allColumns.add(new NSparkTableMeta.SparkTableColumnMeta(field.name(), type,
                    field.getComment().isDefined() ? field.getComment().get() : null));
        }

        return allColumns;
    }

    private void checkTableIsValid(CatalogTable tableMetadata, TableIdentifier tableIdentifier, String tableName) {
        if (CatalogTableType.VIEW().equals(tableMetadata.tableType())) {
            try {
                Installer.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
                SparderEnv.getSparkSession().table(tableIdentifier).queryExecution().analyzed();
            } catch (Throwable e) {
                logger.error("Error for parser view: " + tableName, e);
                throw new RuntimeException("Error for parser view: " + tableName + ", " + e.getMessage()
                        + "(There are maybe syntactic differences between HIVE and SparkSQL)", e);
            }
        }
    }

    private Boolean isRangePartition(CatalogTable tableMetadata) {
        List<NSparkTableMeta.SparkTableColumnMeta> allColumns = getColumns(tableMetadata, tableMetadata.schema(), false);
        return allColumns.stream().collect(Collectors.groupingBy(p -> p.name)).values()
                .stream().anyMatch(p -> p.size() > 1);
    }
}
