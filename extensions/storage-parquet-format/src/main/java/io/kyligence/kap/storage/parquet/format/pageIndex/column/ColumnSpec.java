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

package io.kyligence.kap.storage.parquet.format.pageIndex.column;

public class ColumnSpec {
    private int columnLength;
    private int cardinality;
    private char keyEncodingIdentifier = 'a';
    private char valueEncodingIdentifier = 'a';
    private String columnName;
    private boolean onlyEQIndex;
    private int columnSequence;
    private int totalPageNum = Integer.MIN_VALUE;

    public ColumnSpec(String columnName, int columnLength, int cardinality, boolean onlyEQIndex, int columnSequence) {
        this.columnLength = columnLength;
        this.cardinality = cardinality;
        this.columnName = columnName;
        this.onlyEQIndex = onlyEQIndex;
        this.columnSequence = columnSequence;
    }

    public int getTotalPageNum() {
        return totalPageNum;
    }

    public void setTotalPageNum(int totalPageNum) {
        this.totalPageNum = totalPageNum;
    }

    public char getKeyEncodingIdentifier() {
        return keyEncodingIdentifier;
    }

    public void setKeyEncodingIdentifier(char keyEncodingIdentifier) {
        this.keyEncodingIdentifier = keyEncodingIdentifier;
    }

    public char getValueEncodingIdentifier() {
        return valueEncodingIdentifier;
    }

    public void setValueEncodingIdentifier(char valueEncodingIdentifier) {
        this.valueEncodingIdentifier = valueEncodingIdentifier;
    }

    public String getColumnName() {
        return columnName;
    }

    public void setColumnName(String columnName) {
        this.columnName = columnName;
    }

    public int getCardinality() {
        return cardinality;
    }

    public void setCardinality(int cardinality) {
        this.cardinality = cardinality;
    }

    public boolean isOnlyEQIndex() {
        return onlyEQIndex;
    }

    public void setOnlyEQIndex(boolean onlyEQIndex) {
        this.onlyEQIndex = onlyEQIndex;
    }

    public int getColumnLength() {
        return columnLength;
    }

    public void setColumnLength(int columnLength) {
        this.columnLength = columnLength;
    }

    public int getColumnSequence() {
        return columnSequence;
    }

    public void setColumnSequence(int columnSequence) {
        this.columnSequence = columnSequence;
    }
}
