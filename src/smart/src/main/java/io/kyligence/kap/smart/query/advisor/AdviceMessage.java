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

    private AdviceMessage() {
    }

    public static AdviceMessage getInstance() {
        if (instance == null) {
            instance = new AdviceMessage();
        }
        return instance;
    }

    // default reason
    private static final String DEFAULT_REASON = "Something went wrong. %s";
    private static final String DEFAULT_SUGGEST = "Please contact Kyligence Enterprise technical support for more details.";

    // unexpected token
    private static final String UNEXPECTED_TOKEN = "Syntax error: encountered unexpected token:\" %s\". At line %s, column %s.";

    // bad sql
    private static final String BAD_SQL_REASON = "Syntax error:\"%s\".";
    private static final String BAD_SQL_SUGGEST = "Please correct the SQL.";

    // bad sql table not found
    private static final String BAD_SQL_TABLE_NOT_FOUND_REASON = "Table '%s' not found.";
    private static final String BAD_SQL_TABLE_NOT_FOUND_SUGGEST = "Please add table %s to data source. If this table does exist, mention it as DATABASE.TABLE.";

    // bad sql column not found
    private static final String BAD_SQL_COLUMN_NOT_FOUND_REASON = "Column '%s' not found in any table.";
    private static final String BAD_SQL_COLUMN_NOT_FOUND_SUGGEST = "Please add column %s to data source.";

    // bad sql column not found in table
    private static final String BAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_REASON = "Column '%s' not found in table '%s'.";
    private static final String BAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_SUGGEST = "Please add column %s to table %s in data source.";

    // other model error
    private static final String OTHER_MODEL_INCAPABLE_REASON = "Part of SQL can be answered by this model, error occurs when rest SQL needs help from other models.";
    private static final String OTHER_MODEL_INCAPABLE_SUGGEST = "Please take a look at other models, which including fact table: %s.";

    // ==========================================================

    public String getDefaultReason() {
        return DEFAULT_REASON;
    }

    String getDefaultSuggest() {
        return DEFAULT_SUGGEST;
    }

    String getUnexpectedToken() {
        return UNEXPECTED_TOKEN;
    }

    String getBadSqlReason() {
        return BAD_SQL_REASON;
    }

    String getBadSqlSuggest() {
        return BAD_SQL_SUGGEST;
    }

    String getBadSqlTableNotFoundReason() {
        return BAD_SQL_TABLE_NOT_FOUND_REASON;
    }

    String getBadSqlTableNotFoundSuggest() {
        return BAD_SQL_TABLE_NOT_FOUND_SUGGEST;
    }

    String getBadSqlColumnNotFoundReason() {
        return BAD_SQL_COLUMN_NOT_FOUND_REASON;
    }

    String getBadSqlColumnNotFoundSuggest() {
        return BAD_SQL_COLUMN_NOT_FOUND_SUGGEST;
    }

    String getBadSqlColumnNotFoundInTableReason() {
        return BAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_REASON;
    }

    String getBadSqlColumnNotFoundInTableSuggest() {
        return BAD_SQL_COLUMN_NOT_FOUND_IN_TABLE_SUGGEST;
    }

    String getModelOtherModelIncapableReason() {
        return OTHER_MODEL_INCAPABLE_REASON;
    }

    String getModelOtherModelIncapableSuggest() {
        return OTHER_MODEL_INCAPABLE_SUGGEST;
    }
}