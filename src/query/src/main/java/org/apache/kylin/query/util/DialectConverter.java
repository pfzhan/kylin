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

package org.apache.kylin.query.util;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.kylin.metadata.model.tool.CalciteParser;
import org.apache.kylin.query.calcite.KEDialect;
import org.apache.kylin.source.adhocquery.IPushDownConverter;

import io.kyligence.kap.common.obf.IKeep;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class DialectConverter implements IPushDownConverter, IKeep {

    @Override
    public String convert(String originSql, String project, String defaultSchema) {
        try {
            SqlNode node = CalciteParser.parse(originSql);
            SqlPrettyWriter writer = new SqlPrettyWriter(KEDialect.DEFAULT);
            return writer.format(node);
        } catch (Exception e) {
            log.error("dialect push down converter failed.", e);
        }
        return originSql;
    }
}
