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

package org.apache.kylin.source.hive;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.hcatalog.common.HCatException;
import org.apache.hive.hcatalog.data.HCatRecord;
import org.apache.hive.hcatalog.data.transfer.DataTransferFactory;
import org.apache.hive.hcatalog.data.transfer.HCatReader;
import org.apache.hive.hcatalog.data.transfer.ReadEntity;
import org.apache.hive.hcatalog.data.transfer.ReaderContext;
import org.apache.kylin.source.IReadableTable.TableReader;

/**
 * An implementation of TableReader with HCatalog for Hive table.
 */
public class HiveTableReader implements TableReader {

    private String dbName;
    private String tableName;
    private int currentSplit = -1;
    private ReaderContext readCntxt = null;
    private Iterator<HCatRecord> currentHCatRecordItr = null;
    private HCatRecord currentHCatRecord;
    private int numberOfSplits = 0;
    private Map<String, String> partitionKV = null;

    /**
     * Constructor for reading whole hive table
     * @param dbName
     * @param tableName
     * @throws IOException
     */
    public HiveTableReader(String dbName, String tableName) throws IOException {
        this(dbName, tableName, null);
    }

    /**
     * Constructor for reading a partition of the hive table
     * @param dbName
     * @param tableName
     * @param partitionKV key-value pairs condition on the partition
     * @throws IOException
     */
    public HiveTableReader(String dbName, String tableName, Map<String, String> partitionKV) throws IOException {
        this.dbName = dbName;
        this.tableName = tableName;
        this.partitionKV = partitionKV;
        initialize();
    }

    private void initialize() throws IOException {
        try {
            this.readCntxt = getHiveReaderContext(dbName, tableName, partitionKV);
        } catch (Exception e) {
            e.printStackTrace();
            throw new IOException(e);
        }

        this.numberOfSplits = readCntxt.numSplits();
    }

    @Override
    public boolean next() throws IOException {

        while (currentHCatRecordItr == null || !currentHCatRecordItr.hasNext()) {
            currentSplit++;
            if (currentSplit == numberOfSplits) {
                return false;
            }

            currentHCatRecordItr = loadHCatRecordItr(readCntxt, currentSplit);
        }

        currentHCatRecord = currentHCatRecordItr.next();

        return true;
    }

    @Override
    public String[] getRow() {
        return getRowAsStringArray(currentHCatRecord);
    }

    public List<String> getRowAsList() {
        return getRowAsList(currentHCatRecord);
    }

    public static List<String> getRowAsList(HCatRecord record, List<String> rowValues) {
        List<Object> allFields = record.getAll();
        for (Object o : allFields) {
            rowValues.add((o == null) ? null : o.toString());
        }
        return rowValues;
    }

    public static List<String> getRowAsList(HCatRecord record) {
        return Arrays.asList(getRowAsStringArray(record));
    }

    public static String[] getRowAsStringArray(HCatRecord record) {
        String[] arr = new String[record.size()];
        for (int i = 0; i < arr.length; i++) {
            Object o = record.get(i);
            arr[i] = (o == null) ? null : o.toString();
        }
        return arr;
    }

    @Override
    public void close() throws IOException {
        this.readCntxt = null;
        this.currentHCatRecordItr = null;
        this.currentHCatRecord = null;
        this.currentSplit = -1;
    }

    public String toString() {
        return "hive table reader for: " + dbName + "." + tableName;
    }

    private static ReaderContext getHiveReaderContext(String database, String table, Map<String, String> partitionKV) throws Exception {
        HiveConf hiveConf = new HiveConf(HiveTableReader.class);
        Iterator<Entry<String, String>> itr = hiveConf.iterator();
        Map<String, String> map = new HashMap<String, String>();
        while (itr.hasNext()) {
            Entry<String, String> kv = itr.next();
            map.put(kv.getKey(), kv.getValue());
        }

        ReadEntity entity;
        if (partitionKV == null || partitionKV.size() == 0) {
            entity = new ReadEntity.Builder().withDatabase(database).withTable(table).build();
        } else {
            entity = new ReadEntity.Builder().withDatabase(database).withTable(table).withPartition(partitionKV).build();
        }

        HCatReader reader = DataTransferFactory.getHCatReader(entity, map);
        ReaderContext cntxt = reader.prepareRead();

        return cntxt;
    }

    private static Iterator<HCatRecord> loadHCatRecordItr(ReaderContext readCntxt, int dataSplit) throws HCatException {
        HCatReader currentHCatReader = DataTransferFactory.getHCatReader(readCntxt, dataSplit);

        return currentHCatReader.read();
    }
}
