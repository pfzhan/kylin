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
package io.kyligence.api.catalog;

import io.kyligence.api.annotation.InterfaceStability.Evolving;

import java.util.Objects;

/**
 * Supported primitive type:
 * <ul>
 *     <li>{@code boolean}</li>
 *     <li>{@code tinyint}, {@code byte}</li>
 *     <li>{@code smallint}, {@code short}</li>
 *     <li>{@code int}, {@code integer}</li>
 *     <li>{@code bigint}, {@code long}</li>
 *     <li>{@code float}</li>
 *     <li>{@code date}</li>
 *     <li>{@code timestamp}</li>
 *     <li>{@code string}, {@code char(n)}, {@code varchar(n)}</li>
 *     <li>{@code binary}</li>
 *     <li>{@code decimal}</li>
 * </ul>
 * Supported complex type: map, array, struct.
 */
@Evolving
public class FieldSchema {
    private String name;    // name of the field
    private String type;    // type of the field.
    private String comment;

    public FieldSchema(String name, String type, String comment) {
        this.name = name;
        this.type = type;
        this.comment = comment;
    }
    public String getName() {
        return name;
    }

    public String getType() {
        return type;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldSchema that = (FieldSchema) o;
        return Objects.equals(name, that.name)
                && Objects.equals(type, that.type)
                && Objects.equals(comment, that.comment);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, type, comment);
    }

    @Override
    public String toString() {
        return "FieldSchema{"
                + "name='" + name + '\''
                + ", type='" + type + '\''
                + ", comment='" + comment + '\''
                + '}';
    }
}
