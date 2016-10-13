/**
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

package io.kyligence.kap.cube.index.pinot;

/**
 * Copied from pinot 0.016 (ea6534be65b01eb878cf884d3feb1c6cdb912d2f)
 */
public interface SingleColumnSingleValueReader extends DataFileReader {

    /**
     * fetch the char at a row
     * @param row
     * @return
     */
    char getChar(int row);

    /**
     * fetch short value at a specific row, col
     * @param row
     * @return
     */
    short getShort(int row);

    /**
     * @param row
     * @return
     */
    int getInt(int row);

    /**
     * @param row
     * @return
     */
    long getLong(int row);

    /**
     * @param row
     * @return
     */
    float getFloat(int row);

    /**
     * @param row
     * @return
     */
    double getDouble(int row);

    /**
     * @param row
     * @return
     */
    String getString(int row);

    /**
     * @param row
     * @return
     */
    byte[] getBytes(int row);

    void readValues(int[] rows, int rowStartPos, int rowSize, int[] values, int valuesStartPos);
}
