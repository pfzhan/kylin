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

package io.kyligence.kap.query.engine;

import java.util.Locale;
import java.util.Properties;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.config.NullCollation;
import org.apache.kylin.common.KylinConfig;

/**
 * wrapper for calcite configs defined in kylin props
 */
public class KECalciteConfig extends CalciteConnectionConfigImpl {

    static final ThreadLocal<KECalciteConfig> THREAD_LOCAL = new ThreadLocal<>();

    private KylinConfig kylinConfig;

    private KECalciteConfig(Properties properties, KylinConfig kylinConfig) {
        super(properties);
        this.kylinConfig = kylinConfig;
    }

    public static KECalciteConfig fromKapConfig(KylinConfig kylinConfig) {
        Properties props = new Properties();
        props.putAll(kylinConfig.getCalciteExtrasProperties());
        return new KECalciteConfig(props, kylinConfig);
    }

    @Override
    public NullCollation defaultNullCollation() {
        return NullCollation.LOW;
    }

    @Override
    public boolean caseSensitive() {
        return false;
    }

    @Override
    public Casing unquotedCasing() {
        return this.kylinConfig.getSourceNameCaseSensitiveEnabled() ? Casing.UNCHANGED : super.unquotedCasing();
    }

    public boolean exposeComputedColumn() {
        return kylinConfig.exposeComputedColumn();
    }

    public int getIdentifierMaxLength() {
        return kylinConfig.getMaxModelDimensionMeasureNameLength();
    }

    public String[] operatorTables() {
        return CalciteConnectionProperty.FUN.wrap(properties).getString("standard").toLowerCase(Locale.ROOT).split(",");
    }

    public static KECalciteConfig current() {
        return THREAD_LOCAL.get();
    }
}
