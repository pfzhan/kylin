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
package io.kyligence.kap.secondstorage.ddl.exp;

import io.kyligence.kap.secondstorage.ddl.visitor.RenderVisitor;
import io.kyligence.kap.secondstorage.ddl.visitor.Renderable;

public class ColumnWithType implements Renderable {

    private final String name;
    private final String type;
    private boolean quote;
    private final boolean nullable;

    public ColumnWithType(String name, String type) {
        this(name, type, false, false);
    }

    public ColumnWithType(String name, String type, boolean nullable) {
        this(name, type, nullable, false);
    }

    public ColumnWithType(String name, String type, boolean nullable, boolean quote) {
        this.name = name;
        this.type = type;
        this.quote = quote;
        this.nullable = nullable;
    }

    public String name() {
        return name;
    }

    public String type() {
        return type;
    }

    public boolean quote() {
        return quote;
    }

    public boolean nullable() {
        return nullable;
    }

    public void accept(RenderVisitor visitor) {
        visitor.visit(this);
    }
}
