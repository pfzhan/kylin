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

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kylin.source.datagen;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.common.persistence.RawResource;
import org.apache.kylin.common.persistence.ResourceStore;
import org.apache.kylin.common.util.Bytes;
import org.apache.kylin.metadata.datatype.DataType;
import org.apache.kylin.metadata.model.ColumnDesc;
import org.apache.kylin.metadata.model.JoinDesc;
import org.apache.kylin.metadata.model.JoinTableDesc;
import org.apache.kylin.metadata.model.TableDesc;
import org.apache.kylin.metadata.model.TblColRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import io.kyligence.kap.guava20.shaded.common.io.ByteSource;

import io.kyligence.kap.metadata.model.NDataModel;
import io.kyligence.kap.metadata.model.NDataModelManager;
import lombok.val;

public class ModelDataGenerator {
    private static final Logger logger = LoggerFactory.getLogger(ModelDataGenerator.class);
    final private NDataModel model;
    final private int targetRows;
    final private ResourceStore outputStore;
    final private String outputPath;

    boolean outprint = false; // for debug

    public ModelDataGenerator(NDataModel model, int nRows, ResourceStore outputStore) {
        this(model, nRows, outputStore, "/data");
    }

    private ModelDataGenerator(NDataModel model, int nRows, ResourceStore outputStore, String outputPath) {
        this.model = model;
        this.targetRows = nRows;
        this.outputStore = outputStore;
        this.outputPath = outputPath;
    }

    public void generate() throws IOException {
        Set<TableDesc> generated = new HashSet<>();
        Set<TableDesc> allTableDesc = new LinkedHashSet<>();

        val allTables = model.getJoinTables();
        for (int i = allTables.size() - 1; i >= -1; i--) { // reverse order needed for FK generation
            TableDesc table = (i == -1) ? model.getRootFactTable().getTableDesc()
                    : allTables.get(i).getTableRef().getTableDesc();
            allTableDesc.add(table);

            if (generated.contains(table))
                continue;

            logger.info(String.format(Locale.ROOT, "generating data for %s", table));
            boolean gen = generateTable(table);

            if (gen)
                generated.add(table);
        }

        generateDDL(allTableDesc);
    }

    private boolean generateTable(TableDesc table) throws IOException {
        TableGenConfig config = new TableGenConfig(table, this);
        if (!config.needGen)
            return false;

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintWriter pout = new PrintWriter(new OutputStreamWriter(bout, "UTF-8"));

        generateTableInternal(table, config, pout);

        pout.close();
        bout.close();

        saveResource(bout.toByteArray(), path(table));
        return true;
    }

    private void generateTableInternal(TableDesc table, TableGenConfig config, PrintWriter out) throws IOException {
        ColumnDesc[] columns = table.getColumns();
        ColumnGenerator[] colGens = new ColumnGenerator[columns.length];
        Iterator<String>[] colIters = new Iterator[columns.length];

        // config.rows is either a multiplier (0,1] or an absolute row number
        int tableRows = (int) ((config.rows > 1) ? config.rows : targetRows * config.rows);
        tableRows = Math.max(1, tableRows);

        // same seed for all columns, to ensure composite FK columns generate correct pairs
        long seed = System.currentTimeMillis();

        for (int i = 0; i < columns.length; i++) {
            colGens[i] = new ColumnGenerator(columns[i], tableRows, this);
            colIters[i] = colGens[i].generate(seed);
        }

        for (int i = 0; i < tableRows; i++) {
            for (int c = 0; c < columns.length; c++) {
                if (c > 0)
                    out.print(",");

                String v = colIters[c].next();
                Preconditions.checkState(v == null || !v.contains(","));

                out.print(v);
            }
            out.print("\n");
        }
    }

    private void generateDDL(Set<TableDesc> tables) throws IOException {

        ByteArrayOutputStream bout = new ByteArrayOutputStream();
        PrintWriter pout = new PrintWriter(new OutputStreamWriter(bout, "UTF-8"));

        generateDatabaseDDL(tables, pout);
        generateCreateTableDDL(tables, pout);
        generateLoadDataDDL(tables, pout);

        pout.close();
        bout.close();

        saveResource(bout.toByteArray(), path(model));
    }

    private void generateDatabaseDDL(Set<TableDesc> tables, PrintWriter out) {
        Set<String> dbs = new HashSet<>();
        for (TableDesc t : tables) {
            String db = t.getDatabase();
            if (!StringUtils.isBlank(db) && !"DEFAULT".equals(db))
                dbs.add(db);
        }

        for (String db : dbs) {
            out.print("CREATE DATABASE IF NOT EXISTS " + normHiveIdentifier(db) + ";\n");
        }
        out.print("\n");
    }

    private void generateCreateTableDDL(Set<TableDesc> tables, PrintWriter out) {
        for (TableDesc t : tables) {
            if (t.isView())
                continue;

            out.print("DROP TABLE IF EXISTS " + normHiveIdentifier(t.getIdentity()) + ";\n");

            out.print("CREATE TABLE " + normHiveIdentifier(t.getIdentity()) + "(" + "\n");

            for (int i = 0; i < t.getColumns().length; i++) {
                ColumnDesc col = t.getColumns()[i];
                out.print("    ");
                if (i > 0) {
                    out.print(",");
                }
                out.print(normHiveIdentifier(col.getName()) + " " + hiveType(col.getType()) + "\n");
            }

            out.print(")" + "\n");
            out.print("ROW FORMAT DELIMITED FIELDS TERMINATED BY ','" + "\n");
            out.print("STORED AS TEXTFILE" + ";\n");
            out.print("\n");
        }
    }

    private String normHiveIdentifier(String orig) {
        return "`" + orig + "`";
    }

    private String hiveType(DataType type) {
        String t = type.toString();
        if (t.startsWith("varchar"))
            return "string";
        else if (t.startsWith("integer"))
            return "int";
        else
            return t;
    }

    private void generateLoadDataDDL(Set<TableDesc> tables, PrintWriter out) {
        for (TableDesc t : tables) {
            if (t.isView()) {
                out.print("-- " + t.getIdentity() + " is view \n");
                continue;
            }

            out.print("LOAD DATA LOCAL INPATH '" + t.getIdentity() + ".csv' OVERWRITE INTO TABLE "
                    + normHiveIdentifier(t.getIdentity()) + ";\n");
        }
    }

    public boolean existsInStore(TableDesc table) throws IOException {
        return outputStore.exists(path(table));
    }

    public boolean isPK(ColumnDesc col) {
        for (JoinTableDesc joinTable : model.getJoinTables()) {
            JoinDesc join = joinTable.getJoin();
            for (TblColRef pk : join.getPrimaryKeyColumns()) {
                if (pk.getColumnDesc().equals(col))
                    return true;
            }
        }
        return false;
    }

    public List<String> getPkValuesIfIsFk(ColumnDesc fk) throws IOException {
        val joinTables = model.getJoinTables();
        for (int i = 0; i < joinTables.size(); i++) {
            JoinTableDesc joinTable = joinTables.get(i);
            ColumnDesc pk = findPk(joinTable, fk);
            if (pk == null)
                continue;

            List<String> pkValues = getPkValues(pk);
            if (pkValues != null)
                return pkValues;
        }
        return null;
    }

    private ColumnDesc findPk(JoinTableDesc joinTable, ColumnDesc fk) {
        TblColRef[] fkCols = joinTable.getJoin().getForeignKeyColumns();
        for (int i = 0; i < fkCols.length; i++) {
            if (fkCols[i].getColumnDesc().equals(fk))
                return joinTable.getJoin().getPrimaryKeyColumns()[i].getColumnDesc();
        }
        return null;
    }

    public List<String> getPkValues(ColumnDesc pk) throws IOException {
        if (!existsInStore(pk.getTable()))
            return null;

        List<String> r = new ArrayList<>();

        RawResource resource = outputStore.getResource(path(pk.getTable()));
        try (InputStream is = resource.getByteSource().openStream();
                BufferedReader in = new BufferedReader(new InputStreamReader(is, "UTF-8"))) {

            String line;
            while ((line = in.readLine()) != null) {
                r.add(line.split(",")[pk.getZeroBasedIndex()]);
            }
        }

        return r;
    }

    private void saveResource(byte[] content, String path) {
        System.out.println("Generated " + outputStore.getReadableResourcePath(path));
        if (outprint) {
            System.out.println(Bytes.toString(content));
        }
        outputStore.checkAndPutResource(path, ByteSource.wrap(content), -1);
    }

    private String path(TableDesc table) {
        return outputPath + "/" + table.getIdentity() + ".csv";
    }

    private String path(NDataModel model) {
        return outputPath + "/" + "ddl_" + model.getUuid() + ".sql";
    }

    public NDataModel getModel() {
        return model;
    }

    public static void main(String[] args) throws IOException {
        String projectName = args[0];
        String modelId = args[1];
        int nRows = Integer.parseInt(args[2]);
        String outputDir = args.length > 3 ? args[3] : null;

        KylinConfig conf = KylinConfig.getInstanceFromEnv();
        NDataModel model = NDataModelManager.getInstance(conf, projectName).getDataModelDesc(modelId);
        ResourceStore store = outputDir == null ? ResourceStore.getKylinMetaStore(conf)
                : ResourceStore.getKylinMetaStore(mockup(outputDir));

        ModelDataGenerator gen = new ModelDataGenerator(model, nRows, store);
        gen.generate();
    }

    private static KylinConfig mockup(String outputDir) {
        KylinConfig mockup = KylinConfig.createKylinConfig(KylinConfig.getInstanceFromEnv());
        mockup.setMetadataUrl(new File(outputDir).getAbsolutePath());
        return mockup;
    }
}
