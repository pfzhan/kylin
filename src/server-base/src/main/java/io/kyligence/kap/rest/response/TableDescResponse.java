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

package io.kyligence.kap.rest.response;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kylin.metadata.model.TableDesc;

import java.util.HashMap;
import java.util.Map;

/**
 * A response class to wrap TableDesc
 *
 * @author jianliu
 *
 */
public class TableDescResponse extends TableDesc {
    @JsonProperty("exd")
    Map<String, String> descExd = new HashMap<String, String>();
    @JsonProperty("cardinality")
    Map<String, Long> cardinality = new HashMap<String, Long>();

    /**
     * @return the cardinality
     */
    public Map<String, Long> getCardinality() {
        return cardinality;
    }

    /**
     * @param cardinality
     *            the cardinality to set
     */
    public void setCardinality(Map<String, Long> cardinality) {
        this.cardinality = cardinality;
    }

    /**
     * @return the descExd
     */
    public Map<String, String> getDescExd() {
        return descExd;
    }

    /**
     * @param descExd
     *            the descExd to set
     */
    public void setDescExd(Map<String, String> descExd) {
        this.descExd = descExd;
    }

    /**
     * @param table
     */
    public TableDescResponse(TableDesc table) {
        this.setColumns(table.getColumns());
        this.setDatabase(table.getDatabase());
        this.setName(table.getName());
        this.setSourceType(table.getSourceType());
        this.setUuid(table.getUuid());
        this.setTableType(table.getTableType());
    }

}