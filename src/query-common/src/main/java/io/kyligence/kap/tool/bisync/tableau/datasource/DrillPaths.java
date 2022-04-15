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

import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlElementWrapper;
import com.fasterxml.jackson.dataformat.xml.annotation.JacksonXmlProperty;

public class DrillPaths {

    @JacksonXmlProperty(localName = "drill-path")
    @JacksonXmlElementWrapper(useWrapping = false)
    private List<DrillPath> drillPathList;

    public List<DrillPath> getDrillPathList() {
        return drillPathList;
    }

    public void setDrillPathList(List<DrillPath> drillPathList) {
        this.drillPathList = drillPathList;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (!(o instanceof DrillPaths))
            return false;
        DrillPaths that = (DrillPaths) o;
        return drillPathListEquals(that.getDrillPathList());
    }

    @Override
    public int hashCode() {

        return Objects.hash(getDrillPathList());
    }

    private boolean drillPathListEquals(List<DrillPath> thatDrillPathList) {
        if (getDrillPathList() == thatDrillPathList) {
            return true;
        }
        if (getDrillPathList() != null && thatDrillPathList != null
                && getDrillPathList().size() == thatDrillPathList.size()) {
            Comparator<DrillPath> drillPathComparator = (o1, o2) -> o1.getName().compareTo(o2.getName());
            Collections.sort(getDrillPathList(), drillPathComparator);
            Collections.sort(thatDrillPathList, drillPathComparator);

            boolean flag = true;
            for (int i = 0; i < getDrillPathList().size() && flag; i++) {
                flag = Objects.equals(getDrillPathList().get(i), thatDrillPathList.get(i));
            }
            return flag;
        }
        return false;
    }
}
