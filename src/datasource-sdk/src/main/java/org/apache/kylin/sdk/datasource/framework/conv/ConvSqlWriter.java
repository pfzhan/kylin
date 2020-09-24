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
package org.apache.kylin.sdk.datasource.framework.conv;

import java.sql.SQLException;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlWith;
import org.apache.calcite.sql.SqlWithItem;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

public class ConvSqlWriter extends SqlPrettyWriter {
    private static final Logger logger = LoggerFactory.getLogger(ConvSqlWriter.class);

    private static final SqlOrderBy DUMMY_ORDER_BY_NODE = new SqlOrderBy(SqlParserPos.ZERO,
            new DummySqlNode(SqlParserPos.ZERO),
            new SqlNodeList(Lists.<SqlNode> newArrayList(SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)),
                    SqlParserPos.ZERO),
            null, null);

    private SqlConverter.IConfigurer configurer;
    private FrameImpl lastFrame;

    ConvSqlWriter(SqlConverter.IConfigurer configurer) throws SQLException {
        super(configurer.getSqlDialect());

        this.configurer = configurer;
    }

    @Override
    public void endList(Frame frame) {
        super.endList(frame);
        lastFrame = frame instanceof FrameImpl ? (FrameImpl) frame : null;
    }

    @Override
    public void fetchOffset(SqlNode fetch, SqlNode offset) {
        if (fetch == null && offset == null) {
            return;
        }

        switch (configurer.getPagingType().toUpperCase()) {
            case "ROWNUM":
                doWriteRowNum(fetch, offset);
                break;
            case "FETCH_NEXT":
                doWriteFetchNext(fetch, offset);
                break;
            case "LIMIT_OFFSET":
                doWriteLimitOffset(fetch, offset);
                break;
            default:
                if (getDialect().supportsOffsetFetch()) {
                    doWriteFetchNext(fetch, offset);
                } else {
                    doWriteLimitOffset(fetch, offset);
                }
                break;
        }
    }

    protected void doWriteRowNum(SqlNode fetch, SqlNode offset) {
        // do nothing to ignore limit and offset by now.
    }

    private void doWriteFetchNext(SqlNode fetch, SqlNode offset) {
        if (offset == null && !configurer.allowNoOffset())
            offset = SqlLiteral.createExactNumeric("0", SqlParserPos.ZERO);

        if (fetch != null && !configurer.allowNoOrderByWithFetch() && lastFrame != null
                && lastFrame.getFrameType() != FrameTypeEnum.ORDER_BY_LIST) { // MSSQL requires ORDER_BY list for FETCH clause, so must append one here.
            DUMMY_ORDER_BY_NODE.unparse(this, 0, 0);
        }

        if (offset != null) {
            this.newlineAndIndent();
            final Frame offsetFrame = this.startList(FrameTypeEnum.OFFSET);
            this.keyword("OFFSET");
            offset.unparse(this, -1, -1);
            this.keyword("ROWS");
            this.endList(offsetFrame);
        }
        if (fetch != null) {
            if (!configurer.allowFetchNoRows() && fetch instanceof SqlNumericLiteral)
                if (((SqlNumericLiteral) fetch).toValue().equals("0"))
                    fetch = SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO);

            this.newlineAndIndent();
            final Frame fetchFrame = this.startList(FrameTypeEnum.FETCH);
            this.keyword("FETCH");
            this.keyword("NEXT");
            fetch.unparse(this, -1, -1);
            this.keyword("ROWS");
            this.keyword("ONLY");
            this.endList(fetchFrame);
        }
    }

    private void doWriteLimitOffset(SqlNode fetch, SqlNode offset) {
        // Dialect does not support OFFSET/FETCH clause.
        // Assume it uses LIMIT/OFFSET.
        if (fetch != null) {
            this.newlineAndIndent();
            final Frame fetchFrame = this.startList(FrameTypeEnum.FETCH);
            this.keyword("LIMIT");
            fetch.unparse(this, -1, -1);
            this.endList(fetchFrame);
        }
        if (offset != null) {
            this.newlineAndIndent();
            final Frame offsetFrame = this.startList(FrameTypeEnum.OFFSET);
            this.keyword("OFFSET");
            offset.unparse(this, -1, -1);
            this.endList(offsetFrame);
        }
    }

    @Override
    public void identifier(String name) {
        String convertName = name;
        if (configurer.isCaseSensitive()) {
            convertName = configurer.fixIdentifierCaseSensitive(name);
        }
        if (configurer.enableQuote()) {
            String quoted = getDialect().quoteIdentifier(convertName);
            print(quoted);
            setNeedWhitespace(true);
        } else {
            if (!configurer.skipHandleDefault() && convertName.trim().equalsIgnoreCase("default")) {
                String quoted = getDialect().quoteIdentifier(convertName);
                print(quoted);
                setNeedWhitespace(true);
            } else if (!configurer.enableQuote()) {
                super.identifierWithoutQuote(convertName);
            } else {
                super.identifier(convertName);
            }
        }
    }

    @Override
    public void userDefinedType(SqlDataTypeSpec typeSpec, int leftPrec, int rightPrec) {
        keyword(typeSpec.getTypeName().getSimple());

        // also print precision and scale for user-defined-type
        int precision = typeSpec.getPrecision();
        int scale = typeSpec.getScale();
        if (precision >= 0) {
            final SqlWriter.Frame frame = startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
            this.print(precision);
            if (scale >= 0) {
                this.sep(",", true);
                this.print(scale);
            }
            this.endList(frame);
        }
    }

    private static class DummySqlNode extends SqlNodeList {

        private DummySqlNode(SqlParserPos pos) {
            super(pos);
        }

        @Override
        public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
            writer.sep("");
        }
    }

    @Override
    public boolean inQuery() {
        return this.frame == null || this.frame.getFrameType() == FrameTypeEnum.ORDER_BY
                || this.frame.getFrameType() == FrameTypeEnum.WITH || this.frame.getFrameType() == FrameTypeEnum.SETOP
                || this.frame.getFrameType() == FrameTypeEnum.WITH_ITEM;
    }

    @Override
    public boolean isQuoteAllIdentifiers() {
        return super.isQuoteAllIdentifiers();
    }

    @Override
    public void writeWith(SqlCall call, int leftPrec, int rightPrec) {
        final SqlWith with = (SqlWith) call;
        final SqlWriter.Frame frame = this.startList(SqlWriter.FrameTypeEnum.WITH, "WITH", "");
        for (SqlNode node : with.withList) {
            this.sep(",");
            node.unparse(this, 0, 0);
        }
        outputBetweenWithListAndWithbody();
        with.body.unparse(this, 100, 100);
        this.endList(frame);
    }

    @Override
    public void writeWithItem(SqlCall call, SqlWithItem.SqlWithItemOperator sqlWithItemOperator, int leftPrec,
                              int rightPrec) {
        final SqlWithItem withItem = (SqlWithItem) call;
        leftPrec = sqlWithItemOperator.getLeftPrec();
        rightPrec = sqlWithItemOperator.getRightPrec();
        withItem.name.unparse(this, leftPrec, rightPrec);
        if (withItem.columnList != null) {
            withItem.columnList.unparse(this, leftPrec, rightPrec);
        }
        this.keyword("AS");
        Frame frame = this.startList(FrameTypeEnum.WITH_ITEM, "(", ")");
        withItem.query.unparse(this, 10, 10);
        this.endList(frame);
    }

    protected void outputBetweenWithListAndWithbody() {

    }
}
