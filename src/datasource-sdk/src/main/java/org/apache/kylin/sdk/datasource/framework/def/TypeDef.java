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
package org.apache.kylin.sdk.datasource.framework.def;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.primitives.Ints;

public class TypeDef {
    private static final Pattern P = Pattern
            .compile("\\s*([^\\(\\)]+)\\s*(?:\\(\\s*([^\\s,]+)(?:\\s*,\\s*([^\\s,]+)\\s*)?\\))?\\s*");

    @JacksonXmlProperty(localName = "ID", isAttribute = true)
    private String id;

    @JacksonXmlProperty(localName = "EXPRESSION", isAttribute = true)
    private String expression;

    @JacksonXmlProperty(localName = "MAXPRECISION", isAttribute = true)
    private String maxPrecision;

    // computed fields
    private String name;
    private int defaultPrecision;
    private int defaultScale;

    void init() {
        id = id.toUpperCase(Locale.ROOT);

        Matcher m = P.matcher(expression);
        if (m.matches()) {
            name = m.group(1).toUpperCase(Locale.ROOT);
            Integer p = m.group(2) != null ? Ints.tryParse(m.group(2)) : null;
            Integer s = m.group(3) != null ? Ints.tryParse(m.group(3)) : null;
            defaultPrecision = p != null ? p : -1;
            defaultScale = s != null ? s : -1;
        }
    }

    @VisibleForTesting
    void setExpression(String expression) {
        this.expression = expression;
    }

    @VisibleForTesting
    void setId(String id) {
        this.id = id;
    }

    TypeDef() {
    }

    public String getId() {
        return id;
    }

    public String getExpression() {
        return expression;
    }

    public int getMaxPrecision() {
        try {
            return Integer.parseInt(maxPrecision);
        } catch (Exception e) {
            return Integer.MAX_VALUE;
        }
    }

    public String getName() {
        return name;
    }

    public int getDefaultPrecision() {
        return defaultPrecision;
    }

    public int getDefaultScale() {
        return defaultScale;
    }

    public String buildString(int p, int s) {
        return expression.replaceAll("\\$p", Integer.toString(p)).replaceAll("\\$s", Integer.toString(s));
    }

    public static TypeDef fromString(String kylinTypeName) {
        TypeDef t = new TypeDef();
        t.setId("UNKNOWN");
        t.setExpression(kylinTypeName);
        t.init();
        return t;
    }
}
