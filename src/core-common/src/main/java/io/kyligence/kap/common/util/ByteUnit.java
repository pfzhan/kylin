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

public enum ByteUnit {
    BYTE(1L), KiB(1024L), MiB((long) Math.pow(1024.0D, 2.0D)), GiB((long) Math.pow(1024.0D, 3.0D)), TiB(
            (long) Math.pow(1024.0D, 4.0D)), PiB((long) Math.pow(1024.0D, 5.0D));

    private final long multiplier;

    private ByteUnit(long multiplier) {
        this.multiplier = multiplier;
    }

    public long convertFrom(long d, ByteUnit u) {
        return u.convertTo(d, this);
    }

    public long convertTo(long d, ByteUnit u) {
        if (this.multiplier > u.multiplier) {
            long ratio = this.multiplier / u.multiplier;
            if (9223372036854775807L / ratio < d) {
                throw new IllegalArgumentException("Conversion of " + d + " exceeds Long.MAX_VALUE in " + this.name()
                        + ". Try a larger unit (e.g. MiB instead of KiB)");
            } else {
                return d * ratio;
            }
        } else {
            return d / (u.multiplier / this.multiplier);
        }
    }

    public double toBytes(long d) {
        if (d < 0L) {
            throw new IllegalArgumentException("Negative size value. Size must be positive: " + d);
        } else {
            return (double) (d * this.multiplier);
        }
    }

    public long toKiB(long d) {
        return this.convertTo(d, KiB);
    }

    public long toMiB(long d) {
        return this.convertTo(d, MiB);
    }

    public long toGiB(long d) {
        return this.convertTo(d, GiB);
    }

    public long toTiB(long d) {
        return this.convertTo(d, TiB);
    }

    public long toPiB(long d) {
        return this.convertTo(d, PiB);
    }
}