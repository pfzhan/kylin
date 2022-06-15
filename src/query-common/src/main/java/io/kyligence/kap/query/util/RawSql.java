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

package io.kyligence.kap.query.util;

import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import org.apache.commons.collections.CollectionUtils;
import org.apache.kylin.common.KylinConfig;

import com.google.common.collect.Lists;

import lombok.Getter;

public class RawSql {

    private static final String LINE_SEPARATOR = System.getProperty("line.separator");
    private static final Pattern WHITE_SPACE_PATTERN = Pattern.compile("\\s");
    public static final String SELECT = "select";
    public static final String WITH = "with";
    public static final String EXPLAIN = "explain";

    // User original sql
    @Getter
    private final String sql;
    // Statement block list
    private final List<RawSqlBlock> stmtBlockList;
    // All block list including statement block & comment block
    private final List<RawSqlBlock> allBlockList;
    // Cache
    private String statementStringCache;
    private String fullTextStringCache;

    public RawSql(String sql, List<RawSqlBlock> stmtBlockList, List<RawSqlBlock> allBlockList) {
        this.sql = sql.trim();
        this.stmtBlockList = stmtBlockList;
        this.allBlockList = allBlockList;
        removeStatementEndedSemicolon();
    }

    public String getStatementString() {
        if (statementStringCache != null) {
            return statementStringCache;
        }
        StringBuilder stmt = new StringBuilder();
        int prevEndLine = -1;
        for (RawSqlBlock block : stmtBlockList) {
            if (block.getBeginLine() > prevEndLine) {
                if (prevEndLine != -1) {
                    stmt.append(LINE_SEPARATOR);
                }
                stmt.append(block.getTrimmedText());
            } else if (block.getBeginLine() == prevEndLine) {
                stmt.append(" ");
                stmt.append(block.getTrimmedText());
            }
            prevEndLine = block.getEndLine();
        }
        statementStringCache = stmt.toString();
        return statementStringCache;
    }

    public String getFullTextString() {
        if (fullTextStringCache != null) {
            return fullTextStringCache;
        }
        StringBuilder fullText = new StringBuilder();
        for (RawSqlBlock block : allBlockList) {
            fullText.append(block.getText());
        }
        fullTextStringCache = fullText.toString();
        return fullTextStringCache;
    }

    public void autoAppendLimit(KylinConfig kylinConfig, int limit) {
        autoAppendLimit(kylinConfig, limit, 0);
    }

    public void autoAppendLimit(KylinConfig kylinConfig, int limit, int offset) {
        if (CollectionUtils.isEmpty(allBlockList) || !isSelectStatement()) {
            return;
        }

        //Split keywords and variables from sql by punctuation and whitespace character
        List<String> sqlElements = Lists
                .newArrayList(getStatementString().toLowerCase(Locale.ROOT).split("(?![\\._])\\p{P}|\\s+"));
        boolean limitAppended = false;

        Integer maxRows = kylinConfig.getMaxResultRows();
        if (maxRows != null && maxRows > 0 && (maxRows < limit || limit <= 0)) {
            limit = maxRows;
        }

        if (limit > 0 && !sqlElements.contains("limit")) {
            appendStmtBlock("\nLIMIT " + limit);
            limitAppended = true;
        }

        if (offset > 0 && !sqlElements.contains("offset")) {
            appendStmtBlock("\nOFFSET " + offset);
        }

        // https://issues.apache.org/jira/browse/KYLIN-2649
        int forceLimit;
        if ((forceLimit = kylinConfig.getForceLimit()) > 0 && !limitAppended && !sqlElements.contains("limit")
                && getStatementString().toLowerCase(Locale.ROOT).matches("^select\\s+\\*\\p{all}*")) {
            appendStmtBlock("\nLIMIT " + forceLimit);
        }
    }

    private boolean isSelectStatement() {
        String stmt = getStatementString();
        int startIndex = 0;
        char c;
        while ((c = stmt.charAt(startIndex)) == '(' || WHITE_SPACE_PATTERN.matcher(String.valueOf(c)).matches()) {
            ++startIndex;
        }
        stmt = stmt.substring(startIndex).toLowerCase(Locale.ROOT);
        return stmt.startsWith(SELECT) || (stmt.startsWith(WITH) && stmt.contains(SELECT))
                || (stmt.startsWith(EXPLAIN) && stmt.contains(SELECT));
    }

    private void removeStatementEndedSemicolon() {
        if (CollectionUtils.isEmpty(stmtBlockList)) {
            return;
        }
        RawSqlBlock block = stmtBlockList.get(stmtBlockList.size() - 1);
        String text = block.getText();
        boolean done = false;
        for (int i = text.length() - 1; !done && i >= 0; i--) {
            char c = text.charAt(i);
            if (WHITE_SPACE_PATTERN.matcher(String.valueOf(c)).matches()) {
                continue;
            }
            if (c == ';') {
                text = text.substring(0, i) + text.substring(i + 1);
            } else {
                done = true;
            }
        }
        block.setText(text);
    }

    private void appendStmtBlock(String stmt) {
        appendStmtBlock(stmt, 1);
    }

    private void appendStmtBlock(String stmt, int lines) {
        int lineNo = allBlockList.get(allBlockList.size() - 1).getEndLine() + 1;
        RawSqlBlock block = new RawSqlBlock(stmt, RawSqlBlock.Type.STATEMENT, lineNo, 0, lineNo + lines - 1,
                stmt.length());
        stmtBlockList.add(block);
        allBlockList.add(block);
        clearCache();
    }

    private void clearCache() {
        statementStringCache = null;
        fullTextStringCache = null;
    }
}
