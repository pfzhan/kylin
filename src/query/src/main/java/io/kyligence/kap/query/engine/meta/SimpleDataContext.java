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

package io.kyligence.kap.query.engine.meta;

import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.kylin.common.KylinConfig;

/**
 * a simple data context holder
 */
public class SimpleDataContext implements DataContext {

    private final SchemaPlus rootSchema;
    private final JavaTypeFactory javaTypeFactory;
    private final KylinConfig kylinConfig;
    private final Map<String, Object> contextVars = new HashMap<>();

    public SimpleDataContext(SchemaPlus rootSchema, JavaTypeFactory javaTypeFactory, KylinConfig kylinConfig) {
        this.rootSchema = rootSchema;
        this.javaTypeFactory = javaTypeFactory;
        this.kylinConfig = kylinConfig;
        initContextVars();
    }

    private void initContextVars() {
        putContextVar(Variable.UTC_TIMESTAMP.camelName, System.currentTimeMillis());
        putContextVar(Variable.CURRENT_TIMESTAMP.camelName, System.currentTimeMillis());

        TimeZone tz = TimeZone.getTimeZone(kylinConfig.getTimeZone());
        putContextVar(Variable.LOCAL_TIMESTAMP.camelName, tz.getOffset(System.currentTimeMillis()) + System.currentTimeMillis());
        putContextVar(Variable.TIME_ZONE.camelName, tz);
    }

    @Override
    public SchemaPlus getRootSchema() {
        return rootSchema;
    }

    @Override
    public JavaTypeFactory getTypeFactory() {
        return javaTypeFactory;
    }

    @Override
    public QueryProvider getQueryProvider() {
        return null;
    }

    @Override
    public Object get(String name) {
        return contextVars.get(name);
    }

    public void putContextVar(String name, Object value) {
        contextVars.put(name, value);
    }

    public void setPrepareParam(int idx, Object value) {
        contextVars.put(prepareParamName(idx), value);
    }

    private String prepareParamName(int idx) {
        return String.format("?%d", idx);
    }
}
