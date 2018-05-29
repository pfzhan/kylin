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

package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.key;

public class KeyEncodingFactory {

    public static IKeyEncoding selectEncoding(char identifier, int columnLength, boolean onlyEQ) {
        EncodingType type = EncodingType.fromIdentifier(identifier);
        switch (type) {
        case FIXED_LEN:
            return new FixedLenEncoding(columnLength);
        case LONG:
            return new LongEncoding();
        case INTEGER:
            return new IntEncoding();
        case SHORT:
            return new ShortEncoding();
        case BYTE:
            return new ByteEncoding();
        case AUTO:
            if (!onlyEQ) {
                // keep order
                if (columnLength < 2) {
                    return new ShortEncoding();
                } else if (columnLength < 4) {
                    return new IntEncoding();
                } else if (columnLength < 8) {
                    return new LongEncoding();
                } else {
                    return new FixedLenEncoding(columnLength);
                }
            } else {
                // no need to keep order
                if (columnLength <= 1) {
                    return new ByteEncoding();
                } else if (columnLength <= 2) {
                    return new ShortEncoding();
                } else if (columnLength <= 4) {
                    return new IntEncoding();
                } else if (columnLength <= 8) {
                    return new LongEncoding();
                } else {
                    return new FixedLenEncoding(columnLength);
                }
            }
        default:
            throw new RuntimeException("Unrecognized encoding type: " + identifier);
        }
    }

    public static IKeyEncoding useEncoding(char identifier, int columnLength) {
        EncodingType type = EncodingType.fromIdentifier(identifier);
        switch (type) {
        case FIXED_LEN:
            return new FixedLenEncoding(columnLength);
        case LONG:
            return new LongEncoding();
        case INTEGER:
            return new IntEncoding();
        case SHORT:
            return new ShortEncoding();
        case BYTE:
            return new ByteEncoding();
        default:
            throw new RuntimeException("Unrecognized encoding type: " + identifier);
        }
    }
}
