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


package io.kyligence.kap.common.util;

import java.util.Locale;

import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.util.SqlBasicVisitor;

import com.google.common.collect.ImmutableList;

public class ModifyTableNameSqlVisitor extends SqlBasicVisitor<Object> {
    private final String oldAliasName;
    private final String newAliasName;

    public ModifyTableNameSqlVisitor(String oldAliasName, String newAliasName) {
        this.oldAliasName = oldAliasName;
        this.newAliasName = newAliasName;
    }

    @Override
    public Object visit(SqlIdentifier id) {
        if (id.names.size() == 2) {
            String table = id.names.get(0).toUpperCase(Locale.ROOT).trim();
            String column = id.names.get(1).toUpperCase(Locale.ROOT).trim();
            if (table.equalsIgnoreCase(oldAliasName)) {
                id.names = ImmutableList.of(newAliasName, column);
            }
        }
        return null;
    }
}
