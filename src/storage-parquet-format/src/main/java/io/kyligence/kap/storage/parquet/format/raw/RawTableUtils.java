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

package io.kyligence.kap.storage.parquet.format.raw;

import java.util.ArrayList;
import java.util.List;

import org.apache.kylin.common.KapConfig;
import org.apache.kylin.common.util.ByteArray;

public class RawTableUtils {
    private static byte[] byteMapping;

    static {
        /**
         * byte mapping
         * map 0x00 ~ 0xff --> 0x00 ~ 0x3f */
        byteMapping = new byte[] { 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0X13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x20, 0x21, 0x22, 0X23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0X13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x30, 0x31, 0x32, 0x33, 0x34, 0x20, 0x21, 0x22, 0X23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x35, 0x36, 0x37, 0x38, 0x39, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0X13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f,
                0x20, 0x21, 0x22, 0X23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x30, 0x31, 0x32, 0x33, 0x34, 0x35, 0x36, 0x37, 0x38, 0x39, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11, 0x12, 0X13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x30, 0x31, 0x32, 0x33, 0x34, 0x20, 0x21, 0x22, 0X23, 0x24, 0x25, 0x26, 0x27, 0x28, 0x29, 0x2a, 0x2b, 0x2c, 0x2d, 0x2e, 0x2f, 0x1b, 0x1c, 0x1d, 0x1e, 0x1f, 0x3a, 0x3b, 0x3c, 0x3d, 0x3e, 0x3f, 0x35, 0x36, 0x37, 0x38, 0x39, };
    }

    public static List<byte[]> hash(List<byte[]> value) {
        int hashLength = KapConfig.getInstanceFromEnv().getParquetIndexHashLength();
        return hash(value, hashLength);
    }

    public static List<byte[]> hash(List<byte[]> value, int length) {
        List<byte[]> result = new ArrayList<>(value.size());
        for (byte[] v : value) {
            result.add(hash(v, length));
        }
        return result;
    }

    public static byte[] hash(byte[] value, int length) {
        byte[] result = new byte[length];

        if (value.length <= length) {
            System.arraycopy(value, 0, result, 0, value.length);
        } else {
            System.arraycopy(value, 0, result, 0, length);

            for (int i = length; i < value.length; i++) {
                result[i % length] ^= value[i];
            }
        }

        return result;
    }

    public static ByteArray hash(ByteArray value) {
        int hashLength = KapConfig.getInstanceFromEnv().getParquetIndexHashLength();
        byte[] result = new byte[hashLength];

        if (value.length() <= hashLength) {
            System.arraycopy(value.array(), value.offset(), result, 0, value.length());
            for (int i = value.length(); i < hashLength; i++) {
                result[i] = (byte) 0;
            }
        } else {
            System.arraycopy(value.array(), value.offset(), result, 0, hashLength);

            for (int i = hashLength; i < value.length(); i++) {
                result[i % hashLength] ^= value.array()[i + value.offset()];
            }
        }

        return new ByteArray(result);
    }

    public static ByteArray shrink(ByteArray byteArray, int length) {
        return new ByteArray(shrink(byteArray.array(), byteArray.offset(), byteArray.length(), length));
    }

    public static byte[] shrink(byte[] origin, int bitWidth) {
        return shrink(origin, 0, origin.length, bitWidth);
    }

    /***
     * shrink byte array
     * @param origin original byte array
     * @param offset origin start offset
     * @param length origin length
     * @param bitWidth result byte array width in bits
     * @return shrinked byte array
     */
    public static byte[] shrink(byte[] origin, int offset, int length, int bitWidth) {
        byte[] shrinked = new byte[roundToByte(length * 6)];

        if (origin == null || length == 0) {
            return origin;
        }

        if ((offset + length) > origin.length) {
            return null;
        }

        int si = 0;

        for (int i = offset; i < (offset + length); i += 4) {
            shrinked[si] = (byte) (origin[i] << 2);

            if ((i + 1) < (offset + length)) {
                shrinked[si++] |= (byte) (origin[i + 1] >> 4);
                shrinked[si] = (byte) (origin[i + 1] << 4);
            }

            if ((i + 2) < (offset + length)) {
                shrinked[si++] |= (byte) (origin[i + 2] >> 2);
                shrinked[si] = (byte) (origin[i + 2] << 6);
            }

            if ((i + 3) < (offset + length)) {
                shrinked[si++] |= origin[i + 3];
            }
        }

        shrinked = hash(shrinked, roundToByte(bitWidth));
        if ((bitWidth % 8) == 0) {
            return shrinked;
        }
        shrinked[shrinked.length - 1] &= (byte) (0xFF00 >> (bitWidth % 8));
        return shrinked;
    }

    public static ByteArray toLower(ByteArray byteArray) {
        return new ByteArray(new String(byteArray.array(), byteArray.offset(), byteArray.length()).toLowerCase().getBytes());
    }

    public static int roundToByte(int bitCount) {
        return (bitCount + 7) / 8;
    }
}
