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
package io.kyligence.kap.tool.bisync.tableau.datasource.column;

import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class Column {

    @JacksonXmlProperty(localName = "caption", isAttribute = true)
    private String caption;

    @JacksonXmlProperty(localName = "datatype", isAttribute = true)
    private String datatype;

    @JacksonXmlProperty(localName = "name", isAttribute = true)
    private String name;

    @JacksonXmlProperty(localName = "role", isAttribute = true)
    private String role;

    @JacksonXmlProperty(localName = "type", isAttribute = true)
    private String type;

    @JacksonXmlProperty(localName = "hidden", isAttribute = true)
    private String hidden;

    @JacksonXmlProperty(localName = "ke_cube_used", isAttribute = true)
    private String keCubeUsed;

    @JacksonXmlProperty(localName = "aggregation", isAttribute = true)
    private String aggregation;

    @JacksonXmlProperty(localName = "semantic-role", isAttribute = true)
    private String semanticRole;

    @JacksonXmlProperty(localName = "auto-column", isAttribute = true, namespace = "user")
    private String autoColumn;

    @JacksonXmlProperty(localName = "calculation")
    private Calculation calculation;

    public String getCaption() {
        return caption;
    }

    public void setCaption(String caption) {
        this.caption = caption;
    }

    public String getDatatype() {
        return datatype;
    }

    public void setDatatype(String datatype) {
        this.datatype = datatype;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getRole() {
        return role;
    }

    public void setRole(String role) {
        this.role = role;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Calculation getCalculation() {
        return calculation;
    }

    public void setCalculation(Calculation calculation) {
        this.calculation = calculation;
    }

    public String getHidden() {
        return hidden;
    }

    public void setHidden(String hidden) {
        this.hidden = hidden;
    }

    public String getKeCubeUsed() {
        return keCubeUsed;
    }

    public void setKeCubeUsed(String keCubeUsed) {
        this.keCubeUsed = keCubeUsed;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof Column))
            return false;
        Column column = (Column) o;
        return Objects.equals(getCaption(), column.getCaption()) && Objects.equals(getDatatype(), column.getDatatype())
                && Objects.equals(getName(), column.getName()) && Objects.equals(getRole(), column.getRole())
                && Objects.equals(getType(), column.getType()) && Objects.equals(getHidden(), column.getHidden())
                && Objects.equals(getKeCubeUsed(), column.getKeCubeUsed())
                && Objects.equals(getCalculation(), column.getCalculation());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getCaption(), getDatatype(), getName(), getRole(), getType(), getHidden(),
                getCalculation());
    }
}
