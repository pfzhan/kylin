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
package io.kyligence.kap.tool.bisync.tableau.datasource.connection;

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class Cols {

    @JacksonXmlProperty(localName = "map", isAttribute = true)
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<Col> cols;

    public List<Col> getCols() {
        return cols;
    }

    public void setCols(List<Col> cols) {
        this.cols = cols;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof Cols)) {
            return false;
        }
        Cols cols1 = (Cols) o;
        return colListEquals(cols1.getCols());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getCols());
    }

    private boolean colListEquals(List<Col> thatColList) {
        if (getCols() == thatColList) {
            return true;
        }
        if (getCols() != null && thatColList != null && getCols().size() == thatColList.size()) {
            Comparator<Col> colsComparator = (o1, o2) -> o1.getKey().compareTo(o2.getKey());
            Collections.sort(getCols(), colsComparator);
            Collections.sort(thatColList, colsComparator);

            boolean flag = true;
            for (int i = 0; i < getCols().size() && flag; i++) {
                flag = Objects.equals(getCols().get(i), thatColList.get(i));
            }
            return flag;
        }
        return false;
    }
}
