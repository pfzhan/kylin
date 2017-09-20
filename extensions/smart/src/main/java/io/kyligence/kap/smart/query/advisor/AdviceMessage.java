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

package io.kyligence.kap.smart.query.advisor;

public class AdviceMessage {

    private static AdviceMessage instance = null;

    protected AdviceMessage() {
    }

    public static AdviceMessage getInstance() {
        if (instance == null) {
            instance = new AdviceMessage();
        }
        return instance;
    }

    public String getDEFAULT_REASON() {
        return "Something complex went wrong. %s";
    }

    public String getDEFAULT_SUGGEST() {
        return "Please contact KAP technical support for more details.";
    }

    public String getUNEXPECTED_TOKEN() {
        return "Syntax error: encountered unexpected token:\" %s\". At line %s, column %s.";
    }

    public String getBAD_SQL_REASON() {
        return "Syntax error:\"%s\".";
    }

    public String getBAD_SQL_SUGGEST() {
        return "Please correct the SQL.";
    }

    public String getBAD_SQL_TABLE_NOT_FOUND_REASON() {
        return "Table '%s' not found.";
    }

    public String getBAD_SQL_TABLE_NOT_FOUND_SUGGEST() {
        return "Please add table %s to data source.";
    }

    public String getBAD_SQL_TABLE_CASE_ERR_REASON() {
        return "Database/table/column name within a pair of double quotation should be in upper case.";
    }

    public String getBAD_SQL_TABLE_CASE_ERR_SUGGEST() {
        return "Please change \"%s\" to \"%s\" in SQL.";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_REASON() {
        return "Column '%s' not found in any table.";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_REASON() {
        return "Column '%s' not found in table '%s'.";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_SUGGEST() {
        return "Please add column %s to data source.";
    }

    public String getBAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_SUGGEST() {
        return "Please add column %s to table %s in data source.";
    }

    //bad sequence of join
    public String getMODEL_BAD_JOIN_SEQUENCE_REASON() {
        return "The sequence of join tables are not matched in model %s";
    }

    public String getMODEL_BAD_JOIN_SEQUENCE_SUGGEST() {
        return "Please adjust the sequence of join tables to (%s) in model %s";
    }

    //join type unmatched in model
    public String getMODEL_JOIN_TYPE_UNMATCHED_REASON() {
        return "Join type unmatched. %s from SQL is unmatched with %s from model %s.";
    }

    public String getMODEL_JOIN_TYPE_UNMATCHED_SUGGEST() {
        return "Please ensure the join type from model is matched with SQL.";
    }

    //join condition unmatched in model
    public String getMODEL_JOIN_CONDITION_UNMATCHED_REASON() {
        return "Join condition unmatched. %s from SQL is unmatched with %s from model %s.";
    }

    public String getMODEL_JOIN_CONDITION_UNMATCHED_SUGGEST() {
        return "Please ensure the join condition from model is matched with SQL.";
    }

    //join type and condition unmatched in model
    public String getMODEL_JOIN_TYPE_CONDITION_UNMATCHED_REASON() {
        return "Join unmatched. %s from SQL is unmatched with %s from model %s.";
    }

    public String getMODEL_JOIN_TYPE_CONDITION_UNMATCHED_SUGGEST() {
        return "Please ensure the join from model is matched with SQL.";
    }

    //join table not found in model
    public String getMODEL_JOIN_TABLE_NOT_FOUND_REASON() {
        return "Table %s not found in model %s.";
    }

    public String getMODEL_JOIN_TABLE_NOT_FOUND_SUGGEST() {
        return "Please add table %s in model %s.";
    }

    //dimensions not found in model
    public String getMODEL_NOT_CONTAIN_ALL_DIMENSIONS_REASON() {
        return "Dimensions %s not found in model %s.";
    }

    public String getMODEL_NOT_CONTAIN_ALL_DIMENSIONS_SUGGEST() {
        return "Please add the columns %s as dimension to model %s.";
    }

    //measures not found in cube
    public String getMODEL_NOT_CONTAIN_ALL_MEASURES_REASON() {
        return "Measures %s not found in model %s.";
    }

    public String getMODEL_NOT_CONTAIN_ALL_MEASURES_SUGGEST() {
        return "Please add the columns %s as measure to model %s.";
    }

    //table not found in model
    public String getMODEL_NOT_CONTAIN_ALL_TABLES_REASON() {
        return "Tables %s are not found in model %s.";
    }

    public String getMODEL_NOT_CONTAIN_ALL_TABLES_SUGGEST() {
        return "Please add tables %s to model %s.";
    }

    //dimensions unmatched in model
    public String getMODEL_UNMATCHED_DIMENSIONS_REASON() {
        return "Dimensions %s not found in model %s.";
    }

    public String getMODEL_UNMATCHED_DIMENSIONS_SUGGEST() {
        return "Please add the columns %s as dimension to model %s.";
    }

    //fact table unmatched in model
    public String getMODEL_FACT_TABLE_UNMATCHED_REASON() {
        return "Fact table %s is not found in model %s.";
    }

    public String getMODEL_FACT_TABLE_UNMATCHED_SUGGEST() {
        return "Please set table %s as fact table in model %s.";
    }

    // other model error
    public String getMODEL_OTHER_MODEL_INCAPABLE_REASON() {
        return "Part of SQL can be answered by this model, error occurs when rest SQL needs help from other models.";
    }

    public String getMODEL_OTHER_MODEL_INCAPABLE_SUGGEST() {
        return "Please take a look at other models, which including fact table: %s.";
    }

    //cube not ready
    public String getCUBE_NOT_READY_REASON() {
        return "Cube %s is not ready";
    }

    public String getCUBE_NOT_READY_SUGGEST() {
        return "Please Set cube %s ready";
    }

    //dimensions not found in cube
    public String getCUBE_NOT_CONTAIN_ALL_DIMENSIONS_REASON() {
        return "Dimension %s not found in cube %s.";
    }

    public String getCUBE_NOT_CONTAIN_ALL_DIMENSION_SUGGEST() {
        return "Please add the columns %s as dimension to cube %s.";
    }

    //measures not found in cube
    public String getCUBE_NOT_CONTAIN_ALL_MEASURES_REASON() {
        return "Measure %s not found in cube %s.";
    }

    public String getCUBE_NOT_CONTAIN_ALL_MEASURES_SUGGEST() {
        return "Please add the columns %s as measure to cube %s.";
    }

    //CUBE_BLACK_OUT_REALIZATION
    public String getCUBE_BLACK_OUT_REALIZATION_REASON() {
        return "Cube %s is black out realization.";
    }

    //CUBE_UN_SUPPORT_MASSIN
    public String getCUBE_UN_SUPPORT_MASSIN_REASON() {
        return "Exclude cube %s because only v2 storage + v2 query engine supports massin.";
    }

    public String getCUBE_UN_SUPPORT_MASSIN_SUGGEST() {
        return "No Suggestion";
    }

    //CUBE_UN_SUPPORT_RAWQUERY
    public String getCUBE_UN_SUPPORT_RAWQUERY_REASON() {
        return "Cube cannot answer raw queries.";
    }

    public String getCUBE_UN_SUPPORT_RAWQUERY_SUGGEST() {
        return "Please enable table index to answer raw queries.";
    }

    //CUBE_UNMATCHED_DIMENSION
    public String getCUBE_UNMATCHED_DIMENSIONS_REASON() {
        return "Dimensions %s are not matched in cube %s.";
    }

    public String getCUBE_UNMATCHED_DIMENSIONS_SUGGEST() {
        return "Please add dimensions %s in the cube %s.";
    }

    //column not found in cube
    public String getCUBE_NOT_CONTAIN_ALL_TABLE_REASON() {
        return "Tables %s are not found in cube %s.";
    }

    public String getCUBE_NOT_CONTAIN_ALL_TABLE_SUGGEST() {
        return "Please add tables %s to cube %s.";
    }

    //CUBE_LIMIT_PRECEDE_AGGR
    public String getCUBE_LIMIT_PRECEDE_AGGR_REASON() {
        return "This SQL cannot be answered for a known limitation.";
    }

    public String getCUBE_LIMIT_PRECEDE_AGGR_SUGGEST() {
        return "Please take a look at this FAQ and try to convert SQL to get a better answer. <a href='https://kybot.io/#/kybot/searchDetail/115002099473' target='_blank'>https://kybot.io/#/kybot/searchDetail/115002099473</a>";
    }

    //CUBE_UNMATCHED_AGGREGATION
    public String getCUBE_UNMATCHED_AGGREGATIONS_REASON() {
        return "Measures %s not found in cube %s.";
    }

    public String getCUBE_UNMATCHED_AGGREGATIONS_SUGGEST() {
        return "Please add measures %s in the cube %s.";
    }

    public String getCUBE_OTHER_CUBE_INCAPABLE_REASON() {
        return "Part of SQL can be answered by this cube, error occurs when rest SQL needs help from other cubes.";
    }

    public String getCUBE_OTHER_CUBE_INCAPABLE_SUGGEST() {
        return "Please take a look at other cubes.";
    }

    public String getSPARK_DRIVER_NOT_RUNNING_REASON() {
        return "Spark driver is not running.";
    }

    public String getSPARK_DRIVER_NOT_RUNNING_SUGGEST() {
        return "Please make sure the spark driver is working by running \"$KYLIN_HOME/bin/spark-client.sh start\".";
    }

    public String getNO_REALIZATION_FOUND_REASON() {
        return "Cube not found.";
    }

    public String getNO_REALIZATION_FOUND_SUGGEST() {
        return "Please ensure you have the cube which including fact table %s.%s. Another suggested way is using Verify SQL(action for cube) among the desired cube to shape it as your will.";
    }
}