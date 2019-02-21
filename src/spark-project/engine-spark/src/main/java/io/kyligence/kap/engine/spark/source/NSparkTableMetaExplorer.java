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

import org.apache.spark.sql.SparderEnv;
import org.apache.spark.sql.catalyst.TableIdentifier;
import org.apache.spark.sql.catalyst.catalog.CatalogTable;
import org.apache.spark.sql.catalyst.catalog.SessionCatalog;

import com.google.common.collect.Lists;

import scala.Option;

public class NSparkTableMetaExplorer implements Serializable {

    public NSparkTableMeta getSparkTableMeta(String database, String tableName) {
        SessionCatalog catalog = SparderEnv.getSparkSession().sessionState().catalog();
        try {
            CatalogTable tableMetadata = catalog
                    .getTempViewOrPermanentTableMetadata(TableIdentifier.apply(tableName, Option.apply(database.isEmpty()?null:database)));
            return getnSparkTableMeta(tableName, tableMetadata);
        } catch (Throwable e) {
            throw new RuntimeException("Error for parser Spark table metadata.", e);
        }
    }

    private NSparkTableMeta getnSparkTableMeta(String tableName, CatalogTable tableMetadata) {
        NSparkTableMetaBuilder builder = new NSparkTableMetaBuilder();
        builder.setTableName(tableName);
        List<NSparkTableMeta.SparkTableColumnMeta> allColumns = Lists
                .newArrayListWithCapacity(tableMetadata.schema().size());
        for (org.apache.spark.sql.types.StructField field : tableMetadata.schema().fields()) {
            if (field.getComment().isDefined()) {
                allColumns.add(new NSparkTableMeta.SparkTableColumnMeta(field.name(), field.dataType().simpleString(),
                        field.getComment().get()));
            } else {
                allColumns.add(
                        new NSparkTableMeta.SparkTableColumnMeta(field.name(), field.dataType().simpleString(), null));
            }
        }
        builder.setAllColumns(allColumns);
        builder.setOwner(tableMetadata.owner());
        builder.setCreateTime(tableMetadata.createTime() + "");
        builder.setLastAccessTime(tableMetadata.lastAccessTime() + "");
        builder.setTableType(tableMetadata.tableType().name());

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
        return builder.createSparkTableMeta();
    }
}
