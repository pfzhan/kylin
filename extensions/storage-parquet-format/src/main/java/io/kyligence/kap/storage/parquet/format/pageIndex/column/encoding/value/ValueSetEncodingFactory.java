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

package io.kyligence.kap.storage.parquet.format.pageIndex.column.encoding.value;

public class ValueSetEncodingFactory {
    public static IValueSetEncoding selectEncoding(char identifier, int docCardinality, boolean onlyEQ) {
        EncodingType type = EncodingType.fromIdentifier(identifier);
        switch (type) {
        case ROARING:
            return new MutableRoaringBitmapEncoding();
        case INT_SET:
            return new IntSetEncoding();
        case SHORT_SET:
            return new ShortSetEncoding();
        case AUTO:
            if (onlyEQ && docCardinality > 0 && docCardinality < 4096) {
                return new ShortSetEncoding();
            } else {
                return new MutableRoaringBitmapEncoding();
            }
        default:
            throw new RuntimeException("Unrecognized encoding type: " + identifier);
        }
    }

    public static IValueSetEncoding useEncoding(char identifier) {
        EncodingType type = EncodingType.fromIdentifier(identifier);
        switch (type) {
        case ROARING:
            return new MutableRoaringBitmapEncoding();
        case INT_SET:
            return new IntSetEncoding();
        case SHORT_SET:
            return new ShortSetEncoding();
        default:
            throw new RuntimeException("Unrecognized encoding type: " + identifier);
        }
    }
}
