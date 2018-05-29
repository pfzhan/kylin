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

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kylin.common.KylinConfig;
import org.apache.kylin.metadata.model.ColumnDesc;

import io.kyligence.kap.metadata.NTableMetadataManager;

public class ColumnGenConfig {

    public static final String FK = "FK";
    public static final String ID = "ID";
    public static final String RAND = "RAND";
    public static final String $RANDOM = "${RANDOM}";

    // discrete values
    boolean isDiscrete;
    boolean isFK;
    List<String> values;

    // random
    boolean isRandom;
    String randFormat;
    int randStart;
    int randEnd;

    // ID
    boolean isID;
    int idStart;

    // general
    int cardinality;
    boolean genNull;
    double genNullPct;
    String genNullStr;
    boolean order;
    boolean unique;

    public ColumnGenConfig(ColumnDesc col, ModelDataGenerator modelGen) throws IOException {
        init(col, modelGen);
    }

    private void init(ColumnDesc col, ModelDataGenerator modelGen) throws IOException {

        Map<String, String> config = Util.parseEqualCommaPairs(col.getDataGen(), "values");

        values = Arrays.asList(Util.parseString(config, "values", "").split("[|]"));

        List<String> pkValues = modelGen.getPkValuesIfIsFk(col);

        if (FK.equals(values.get(0)) || (values.get(0).isEmpty() && pkValues != null)) {
            isFK = true;
            values = getPkValues(modelGen, config, pkValues);
        } else if (ID.equals(values.get(0))) {
            isID = true;
            idStart = (values.size() > 1) ? Integer.parseInt(values.get(1)) : 0;
        } else if (RAND.equals(values.get(0)) || values.get(0).isEmpty()) {
            isRandom = true;
            randFormat = (values.size() > 1) ? values.get(1) : "";
            randStart = (values.size() > 2) ? Integer.parseInt(values.get(2)) : 0;
            randEnd = (values.size() > 3) ? Integer.parseInt(values.get(3)) : 0;
        } else {
            isDiscrete = true;
        }

        cardinality = Util.parseInt(config, "card", guessCardinality(col.getName()));
        genNull = Util.parseBoolean(config, "null", guessGenNull(col.getName()));
        genNullPct = Util.parseDouble(config, "nullpct", 0.01);
        genNullStr = Util.parseString(config, "nullstr", "\\N"); // '\N' is null in hive
        order = Util.parseBoolean(config, "order", false);
        unique = Util.parseBoolean(config, "uniq", modelGen.isPK(col));
    }

    private List<String> getPkValues(ModelDataGenerator modelGen, Map<String, String> config, List<String> dftPkValues)
            throws IOException {
        String pkColName = config.get("pk");
        if (pkColName == null)
            return dftPkValues;

        int cut = pkColName.lastIndexOf('.');
        String pkTableName = pkColName.substring(0, cut);
        pkColName = pkColName.substring(cut + 1);

        KylinConfig kylinConfig = modelGen.getModel().getConfig();
        String project = modelGen.getModel().getProject();
        ColumnDesc pkcol = NTableMetadataManager.getInstance(kylinConfig, modelGen.getModel().getProject())//
                .getTableDesc(pkTableName).findColumnByName(pkColName);
        return modelGen.getPkValues(pkcol);
    }

    private int guessCardinality(String col) {
        for (String s : col.split("_")) {
            if (s.startsWith("C")) {
                try {
                    return Integer.parseInt(s.substring(1));
                } catch (Exception ex) {
                    // ok
                }
            }
        }
        return 0;
    }

    private boolean guessGenNull(String col) {
        return col.contains("_NULL");
    }

}
