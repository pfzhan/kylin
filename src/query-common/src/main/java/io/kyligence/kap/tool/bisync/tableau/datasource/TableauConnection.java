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

import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

import io.kyligence.kap.tool.bisync.tableau.datasource.connection.Cols;
import io.kyligence.kap.tool.bisync.tableau.datasource.connection.NamedConnectionList;
import io.kyligence.kap.tool.bisync.tableau.datasource.connection.metadata.MetadataRecordList;
import io.kyligence.kap.tool.bisync.tableau.datasource.connection.relation.Relation;

public class TableauConnection {

    @JacksonXmlProperty(localName = "class", isAttribute = true)
    private String className;

    @JacksonXmlProperty(localName = "named-connections")
    private NamedConnectionList namedConnectionList;

    @JacksonXmlProperty(localName = "relation")
    private Relation relation;

    @JacksonXmlProperty(localName = "cols")
    private Cols cols;

    @JacksonXmlProperty(localName = "metadata-records")
    private MetadataRecordList metadataRecords;

    public String getClassName() {
        return className;
    }

    public void setClassName(String className) {
        this.className = className;
    }

    public NamedConnectionList getNamedConnectionList() {
        return namedConnectionList;
    }

    public void setNamedConnectionList(NamedConnectionList namedConnectionList) {
        this.namedConnectionList = namedConnectionList;
    }

    public Relation getRelation() {
        return relation;
    }

    public void setRelation(Relation relation) {
        this.relation = relation;
    }

    public Cols getCols() {
        return cols;
    }

    public void setCols(Cols cols) {
        this.cols = cols;
    }

    public MetadataRecordList getMetadataRecords() {
        return metadataRecords;
    }

    public void setMetadataRecords(MetadataRecordList metadataRecords) {
        this.metadataRecords = metadataRecords;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableauConnection)) {
            return false;
        }
        TableauConnection that = (TableauConnection) o;
        return Objects.equals(getNamedConnectionList(), that.getNamedConnectionList())
                && Objects.equals(getRelation(), that.getRelation()) && Objects.equals(getCols(), that.getCols());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getNamedConnectionList(), getRelation(), getCols());
    }
}
