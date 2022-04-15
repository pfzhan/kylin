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
package io.kyligence.kap.tool.bisync.tableau.datasource;

import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class DrillPath {

    @JacksonXmlProperty(localName = "name", isAttribute = true)
    private String name;

    @JacksonXmlProperty(localName = "field")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<String> fields;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public List<String> getFields() {
        return fields;
    }

    public void setFields(List<String> fields) {
        this.fields = fields;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof DrillPath)) {
            return false;
        }
        DrillPath drillPath = (DrillPath) o;
        return Objects.equals(getName(), drillPath.getName()) && Objects.equals(getFields(), drillPath.getFields());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getName(), getFields());
    }

    private boolean fieldsEquals(List<String> thatFields) {
        if (getFields() == thatFields) {
            return true;
        }
        if (getFields() != null && thatFields != null && getFields().size() == thatFields.size()) {
            boolean flag = true;
            for (int i = 0; i < getFields().size() && flag; i++) {
                flag = Objects.equals(getFields().get(i), thatFields.get(i));
            }
            return flag;
        }
        return false;
    }
}
