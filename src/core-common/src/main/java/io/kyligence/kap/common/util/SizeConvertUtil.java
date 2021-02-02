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
package io.kyligence.kap.common.util;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.common.collect.ImmutableMap;

public class SizeConvertUtil {

    private static final ImmutableMap<String, ByteUnit> byteSuffixes;

    private SizeConvertUtil() {
        throw new IllegalStateException("Wrong usage for utility class.");
    }

    public static String getReadableFileSize(long size) {
        if (size <= 0)
            return "0";
        final String[] units = new String[] { "B", "KB", "MB", "GB", "TB", "PB", "EB" };
        int digitGroups = (int) (Math.log10(size) / Math.log10(1024));
        return new DecimalFormat("#,##0.#", DecimalFormatSymbols.getInstance(Locale.getDefault(Locale.Category.FORMAT)))
                .format(size / Math.pow(1024, digitGroups)) + " " + units[digitGroups];
    }

    public static long byteStringAsMb(String str) {
        return byteStringAs(str, ByteUnit.MiB);
    }

    public static long byteStringAs(String str, ByteUnit unit) {
        String lower = str.toLowerCase(Locale.ROOT).trim();

        try {
            Matcher m = Pattern.compile("([0-9]+)([a-z]+)?").matcher(lower);
            Matcher fractionMatcher = Pattern.compile("([0-9]+\\.[0-9]+)([a-z]+)?").matcher(lower);
            if (m.matches()) {
                long val = Long.parseLong(m.group(1));
                String suffix = m.group(2);
                if (suffix != null && !byteSuffixes.containsKey(suffix)) {
                    throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"");
                } else {
                    return unit.convertFrom(val, suffix != null ? (ByteUnit) byteSuffixes.get(suffix) : unit);
                }
            } else if (fractionMatcher.matches()) {
                throw new NumberFormatException(
                        "Fractional values are not supported. Input was: " + fractionMatcher.group(1));
            } else {
                throw new NumberFormatException("Failed to parse byte string: " + str);
            }
        } catch (NumberFormatException var8) {
            String byteError = "Size must be specified as bytes (b), kibibytes (k), mebibytes (m), gibibytes (g), tebibytes (t), or pebibytes(p). E.g. 50b, 100k, or 250m.";
            throw new NumberFormatException(byteError + "\n" + var8.getMessage());
        }
    }

    static {
        byteSuffixes = ImmutableMap.<String, ByteUnit> builder().put("b", ByteUnit.BYTE).put("k", ByteUnit.KiB)
                .put("kb", ByteUnit.KiB).put("m", ByteUnit.MiB).put("mb", ByteUnit.MiB).put("g", ByteUnit.GiB)
                .put("gb", ByteUnit.GiB).put("t", ByteUnit.TiB).put("tb", ByteUnit.TiB).put("p", ByteUnit.PiB)
                .put("pb", ByteUnit.PiB).build();
    }

}
